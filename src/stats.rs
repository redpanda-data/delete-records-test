use crate::config::Config;
use chrono::{DateTime, Utc};
use log::{info, trace};
use rdkafka::consumer::Consumer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::select;
use tokio_util::sync::CancellationToken;

pub type PartitionId = i32;
pub type Offset = i64;

#[derive(Clone, Serialize, Deserialize)]
pub struct TPInfo {
    pub topic: String,
    pub partition: PartitionId,
    pub lwm: i64,
    pub hwm: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum ErrorReport {
    Infrastructure(String),
    Producer(String),
    Consumer(String),
    DeleteRecord(String),
}

impl fmt::Display for ErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorReport::Infrastructure(m) => write!(f, "Infrastructure error: {}", m),
            ErrorReport::Producer(m) => write!(f, "Producer error: {}", m),
            ErrorReport::Consumer(m) => write!(f, "Consumer error: {}", m),
            ErrorReport::DeleteRecord(m) => write!(f, "Delete Record error: {}", m),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ErrorEvent {
    report: ErrorReport,
    time: SystemTime,
}

pub type ReporterKey = String;

type ErrorMap = HashMap<ReporterKey, ErrorEvent>;
type PartitionInfoMap = HashMap<PartitionId, TPInfo>;
type TPInfoMap = HashMap<String, PartitionInfoMap>;
pub type PartitionOffsetMap = HashMap<PartitionId, Offset>;
pub type TopicPartitionOffsetMap = HashMap<String, PartitionOffsetMap>;

#[derive(Clone)]
pub struct StatsHandle {
    inner: Arc<Mutex<Stats>>,
    start_time: Instant,
}

impl StatsHandle {
    pub fn new(i: Arc<Mutex<Stats>>) -> Self {
        let start_time = i.lock().unwrap().start_time;
        Self {
            inner: i,
            start_time,
        }
    }

    pub fn current_tp_info(&self) -> TPInfoMap {
        self.inner.lock().unwrap().current_tp_info()
    }

    pub fn get_status(&self) -> StatsStatus {
        self.inner.lock().unwrap().get_status()
    }

    pub fn update_tp_info(&self) -> Result<(), String> {
        self.inner.lock().unwrap().update_tp_info()
    }

    pub fn report_deleted_records(&self, offsets: TopicPartitionOffsetMap) {
        self.inner.lock().unwrap().report_deleted_records(offsets);
    }

    pub fn report_issue(&self, k: ReporterKey, r: ErrorReport) {
        self.inner.lock().unwrap().report_issue(k, r)
    }

    pub fn last_removed_offsets(&self) -> Option<TopicPartitionOffsetMap> {
        self.inner.lock().unwrap().last_removed_offsets()
    }
}

#[derive(Serialize, Deserialize)]
pub struct StatsStatus {
    updated_at: String,
    tp_info: TPInfoMap,
    errors: ErrorMap,
    last_removed_offsets: Option<TopicPartitionOffsetMap>,
}

pub struct Stats {
    config: Arc<Config>,
    start_time: Instant,
    tp_info: TPInfoMap,
    errors: ErrorMap,
    last_removed_offsets: Option<TopicPartitionOffsetMap>,
}

impl Stats {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        let tp_info = Self::populate_initial_tp_info(config.as_ref())?;
        Ok(Self {
            config,
            start_time: Instant::now(),
            tp_info,
            errors: HashMap::new(),
            last_removed_offsets: None,
        })
    }

    fn current_tp_info(&self) -> TPInfoMap {
        self.tp_info.clone()
    }

    fn update_tp_info(&mut self) -> Result<(), String> {
        self.tp_info = Self::populate_initial_tp_info(self.config.clone().as_ref())?;
        Ok(())
    }

    fn populate_initial_tp_info(config: &Config) -> Result<TPInfoMap, String> {
        let base_consumer = config.make_base_consumer().map_err(|e| format!("{}", e))?;
        let topic = &config.base_config.topic;
        let metadata = base_consumer
            .fetch_metadata(Some(topic.as_str()), Duration::from_millis(1000))
            .map_err(|e| format!("{}", e))?;

        if metadata.topics().is_empty() || metadata.topics()[0].partitions().is_empty() {
            Err(format!("Topic '{}' does not exist", topic))
        } else {
            let partitions = metadata.topics()[0].partitions();
            let mut topic_map = TPInfoMap::with_capacity(1);

            topic_map.insert(
                config.base_config.topic.clone(),
                PartitionInfoMap::with_capacity(partitions.len()),
            );
            let tp_info = topic_map.get_mut(topic).unwrap();
            for p in partitions {
                trace!("Fetching LWM/HWM for {}/{}", topic, p.id());
                let (lwm, hwm) = base_consumer
                    .fetch_watermarks(topic.as_str(), p.id(), Duration::from_millis(1000))
                    .map_err(|e| format!("{}", e))?;
                trace!("{}/{}: ({}/{})", topic, p.id(), lwm, hwm);
                tp_info.insert(
                    p.id(),
                    TPInfo {
                        topic: config.base_config.topic.clone(),
                        partition: p.id(),
                        lwm,
                        hwm,
                    },
                );
            }
            Ok(topic_map)
        }
    }

    fn get_status(&self) -> StatsStatus {
        let dt: DateTime<Utc> = SystemTime::now().into();
        StatsStatus {
            updated_at: dt.to_rfc3339(),
            tp_info: self.tp_info.clone(),
            errors: self.errors.clone(),
            last_removed_offsets: self.last_removed_offsets.clone(),
        }
    }

    fn report_deleted_records(&mut self, offset_map: TopicPartitionOffsetMap) {
        self.last_removed_offsets.replace(offset_map);
    }

    fn report_issue(&mut self, k: ReporterKey, r: ErrorReport) {
        self.errors.insert(
            k,
            ErrorEvent {
                report: r,
                time: SystemTime::now(),
            },
        );
    }

    fn last_removed_offsets(&self) -> Option<TopicPartitionOffsetMap> {
        self.last_removed_offsets.clone()
    }
}

pub async fn monitor_water_marks(
    stats_handle: StatsHandle,
    cancel_token: CancellationToken,
    interval: Duration,
) {
    let mut running = true;

    while running {
        select! {
            _ = cancel_token.cancelled() => {
                info!("monitor_water_marks cancelled");
                running = false;
            }
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = stats_handle.update_tp_info() {
                    stats_handle.report_issue(String::from("MonitorWaterMarks"), ErrorReport::Consumer(e))
                }
            }
        }
    }
}
