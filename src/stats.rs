use crate::config::Config;
use log::{info, trace};
use rdkafka::consumer::Consumer;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::select;
use tokio_util::sync::CancellationToken;

pub type PartitionId = i32;

pub struct TPInfo {
    partition: PartitionId,
    lwm: i64,
    hwm: i64,
}

pub enum ErrorReport {
    ProducerError(String),
    ConsumerError(String),
}

impl fmt::Display for ErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorReport::ProducerError(m) => write!(f, "Producer error: {}", m),
            ErrorReport::ConsumerError(m) => write!(f, "Consumer error: {}", m),
        }
    }
}

struct ErrorEvent {
    report: ErrorReport,
    time: Instant,
}

pub type ReporterKey = String;

type ErrorMap = HashMap<ReporterKey, ErrorEvent>;
type TPInfoMap = HashMap<PartitionId, TPInfo>;

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

    pub fn update_tp_info(&self) -> Result<(), String> {
        self.inner.lock().unwrap().update_tp_info()
    }

    pub fn report_issue(&self, k: ReporterKey, r: ErrorReport) {
        self.inner.lock().unwrap().report_issue(k, r)
    }
}

pub struct Stats {
    config: Arc<Config>,
    start_time: Instant,
    tp_info: Mutex<TPInfoMap>,
    errors: ErrorMap,
}

impl Stats {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        let tp_info = Self::populate_initial_tp_info(config.as_ref())?;
        Ok(Self {
            config,
            start_time: Instant::now(),
            tp_info: Mutex::new(tp_info),
            errors: HashMap::new(),
        })
    }

    fn update_tp_info(&mut self) -> Result<(), String> {
        let tp_info = Self::populate_initial_tp_info(self.config.clone().as_ref())?;
        let mut locked = self.tp_info.lock().expect("Failed unlocking");
        *locked = tp_info;
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
            let mut tp_info = TPInfoMap::with_capacity(partitions.len());
            for p in partitions {
                trace!("Fetching LWM/HWM for {}/{}", topic, p.id());
                let (lwm, hwm) = base_consumer
                    .fetch_watermarks(topic.as_str(), p.id(), Duration::from_millis(1000))
                    .map_err(|e| format!("{}", e))?;
                trace!("{}/{}: ({}/{})", topic, p.id(), lwm, hwm);
                tp_info.insert(
                    p.id(),
                    TPInfo {
                        partition: p.id(),
                        lwm,
                        hwm,
                    },
                );
            }
            Ok(tp_info)
        }
    }

    fn report_issue(&mut self, k: ReporterKey, r: ErrorReport) {
        self.errors.insert(
            k,
            ErrorEvent {
                report: r,
                time: Instant::now(),
            },
        );
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
                    stats_handle.report_issue(String::from("MonitorWaterMarks"), ErrorReport::ConsumerError(e))
                }
            }
        }
    }
}
