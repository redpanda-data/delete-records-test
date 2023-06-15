use crate::config::{Config, DeleteRecordPositionConfig};
use crate::stats::ErrorReport::DeleteRecord;
use crate::stats::{
    ErrorReport, Offset, PartitionId, PartitionOffsetMap, StatsHandle, TopicPartitionOffsetMap,
};
use log::{debug, info, warn};
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::TopicPartitionList;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeleteRecordPosition {
    /// Delete all offsets
    All,
    /// Delete halfway between HWM and LWM
    Halfway,
    /// Delete one after LWM (single offset deletion)
    Single,
    /// Specify partition and offset for deletion
    Specific(Vec<(PartitionId, Offset)>),
}

impl From<DeleteRecordPositionConfig> for DeleteRecordPosition {
    fn from(value: DeleteRecordPositionConfig) -> Self {
        match value {
            DeleteRecordPositionConfig::All => Self::All,
            DeleteRecordPositionConfig::Halfway => Self::Halfway,
            DeleteRecordPositionConfig::Single => Self::Single,
        }
    }
}

impl fmt::Display for DeleteRecordPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DeleteRecordPosition::All => write!(f, "All"),
            DeleteRecordPosition::Halfway => write!(f, "Halfway"),
            DeleteRecordPosition::Single => write!(f, "Single"),
            DeleteRecordPosition::Specific(val) => {
                write!(f, "Specific: ")?;
                for (p, o) in val {
                    write!(f, "({}, {})", *p, *o)?;
                }
                Ok(())
            }
        }
    }
}

fn calc_record_to_delete(
    partition_id: PartitionId,
    lwm: i64,
    hwm: i64,
    delete_record_position: DeleteRecordPosition,
) -> Option<i64> {
    match delete_record_position {
        DeleteRecordPosition::All => Some(hwm),
        DeleteRecordPosition::Halfway => Some((hwm - lwm) / 2 + lwm),
        DeleteRecordPosition::Single => Some(lwm + 1),
        DeleteRecordPosition::Specific(e) => {
            let res: Vec<_> = e
                .into_iter()
                .filter(|(part_id, _)| partition_id == *part_id)
                .collect();
            if res.is_empty() {
                None
            } else {
                Some(res[0].1)
            }
        }
    }
}

async fn record_deleter(
    admin_client: &AdminClient<DefaultClientContext>,
    stats_handle: StatsHandle,
    delete_record_position: DeleteRecordPosition,
    topic: String,
) {
    info!(
        "Starting record deletion at topic {} using strategy {}",
        topic, delete_record_position
    );
    let current_tp_info = stats_handle.current_tp_info();
    let mut removed_offsets_map = TopicPartitionOffsetMap::with_capacity(1);

    let mut tpl = TopicPartitionList::with_capacity(current_tp_info.len());

    let partition_info = current_tp_info.get(&topic).unwrap();
    removed_offsets_map.insert(
        topic.clone(),
        PartitionOffsetMap::with_capacity(partition_info.len()),
    );
    let offset_map = removed_offsets_map.get_mut(&topic).unwrap();

    for (partition_id, tp_info) in partition_info.iter() {
        if let Some(record) = calc_record_to_delete(
            *partition_id,
            tp_info.lwm,
            tp_info.hwm,
            delete_record_position.clone(),
        ) {
            tpl.add_partition_offset(
                topic.as_str(),
                *partition_id,
                rdkafka::Offset::Offset(record),
            )
            .expect("Failed to insert partition offset");
            offset_map.insert(*partition_id, record);
            debug!("Delete record {} at {}/{}", record, topic, partition_id);
        } else {
            debug!("Skipping {}/{}", topic, partition_id);
        }
    }

    match admin_client
        .delete_records(&tpl, &AdminOptions::new())
        .await
    {
        Ok(results) => {
            let mut errors = false;
            for res in results.into_iter().as_ref() {
                if let Err((_, code)) = res {
                    errors = true;
                    warn!("There was an issue deleting a record: {}", code);
                    stats_handle.report_issue(
                        String::from("Delete Record Result"),
                        DeleteRecord(format!("{}", code)),
                    )
                }
            }

            if !errors {
                stats_handle.report_deleted_records(removed_offsets_map);
            }
        }
        Err(e) => {
            warn!("There was an issue deleting a record: {}", e);
            stats_handle.report_issue(
                String::from("Delete Records"),
                ErrorReport::DeleteRecord(format!("{}", e)),
            )
        }
    }
}

pub async fn record_deleter_worker(
    config: Arc<Config>,
    stats_handle: StatsHandle,
    cancel_token: CancellationToken,
    mut trigger_record_delete: Receiver<DeleteRecordPosition>,
) {
    let mut running = true;

    let admin_client = config.make_admin().expect("Failed to create admin client");

    while running {
        select! {
            _ = cancel_token.cancelled() => {
                warn!("Record deleter worker stopped");
                running = false;
            }
            pos = trigger_record_delete.recv() => {
                if let Some(pos) = pos {
                    record_deleter(&admin_client,
                        stats_handle.clone(),
                        pos,
                        config.base_config.topic.clone()).await
                }

            }
        }
    }
}

pub async fn record_deleter_timer_worker(
    stats_handle: StatsHandle,
    delete_record_position: DeleteRecordPosition,
    delete_record_period: Duration,
    cancel_token: CancellationToken,
    trigger_record_delete: Sender<DeleteRecordPosition>,
) {
    let mut running = true;

    while running {
        select! {
            _ = cancel_token.cancelled() => {
                warn!("Offset delete timer stopped");
                running = false;
            }
            _ = tokio::time::sleep(delete_record_period) => {
                if let Err(e) = trigger_record_delete.send(delete_record_position.clone()).await {
                    stats_handle.report_issue(String::from("record_deleter_timer_worker"), ErrorReport::Infrastructure(format!("{}", e)))
                }
            }
        }
    }
}
