extern crate core;

mod config;
mod producer;
mod record_deleter;
mod stats;
mod web;

use crate::config::{CompressionType, Config, ConfigBuilder, DeleteRecordPosition, Payload};
use crate::producer::producers;
use crate::record_deleter::{record_deleter_timer_worker, record_deleter_worker};
use crate::stats::{monitor_water_marks, Stats, StatsHandle};
use crate::web::server;
use clap::Parser;
use log::{debug, error, info, trace, warn};
use rdkafka::consumer::Consumer;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{num, process};
use tokio::sync::mpsc;
use tokio::{select, signal};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, help = "Brokers")]
    brokers: String,
    #[arg(short, long, help = "Existing topic name")]
    topic: String,
    #[arg(short, long, help = "HTTP port", value_parser = port_in_range, default_value_t = 7778)]
    port: u16,
    #[arg(long, help = "Username")]
    username: Option<String>,
    #[arg(long, help = "Password")]
    password: Option<String>,
    #[arg(long)]
    sasl_mechanism: Option<String>,
    #[arg(long)]
    enable_tls: bool,
    // payload
    #[arg(long, help = "Generate compressible payload")]
    compressible_payload: bool,
    #[clap(long, help = "Minimum record size")]
    min_record_size: Option<usize>,
    #[arg(long, default_value_t = 16384, help = "Maximum record size")]
    max_record_size: usize,
    #[arg(long, help = "Range of keys", default_value_t = 1000)]
    keys: u64,
    // delete record settings
    #[arg(long, help = "Position to delete record", value_enum)]
    delete_record_position: DeleteRecordPosition,
    #[arg(long, help = "Frequency of running delete record in seconds", value_parser = parse_duration)]
    delete_record_period_sec: Duration,
    // producer
    #[arg(long, help = "Compression Type", value_enum)]
    compression_type: Option<CompressionType>,
    #[arg(long, help = "Producer properties (as key=value for librdkafka)")]
    producer_properties: Vec<String>,
    #[arg(long, help = "Produce throughput in bps")]
    produce_throughput_bps: Option<u32>,
    #[arg(long, help = "Produce timeout in milliseconds", default_value_t = 1000)]
    timeout_ms: u64,
    #[arg(long, help = "Number of producers", default_value_t = 1)]
    num_producers: usize,
    // consumer settings
    #[arg(long, help = "Consumer properties (as key=value for librdkafka)")]
    consumer_properties: Vec<String>,
    #[arg(long, help = "Consume throughput in MBps")]
    consume_throughput_mbps: Option<usize>,
    #[arg(long, help = "Number of consumers", default_value_t = 1)]
    num_consumers: usize,
    #[arg(long, help = "Random consumption rather than sequential")]
    rand: bool,
}

const PORT_RANGE: RangeInclusive<usize> = 1..=65535;

fn parse_duration(s: &str) -> Result<Duration, num::ParseIntError> {
    let seconds = s.parse()?;
    Ok(Duration::from_secs(seconds))
}

fn port_in_range(s: &str) -> Result<u16, String> {
    let port: usize = s
        .parse()
        .map_err(|_| format!("`{s}` isn't a port number"))?;

    if PORT_RANGE.contains(&port) {
        Ok(port as u16)
    } else {
        Err(format!(
            "port not in range {}-{}",
            PORT_RANGE.start(),
            PORT_RANGE.end()
        ))
    }
}

fn validate_topic_exists(config: &Config) -> Result<(), String> {
    debug!("Fetching metadata for topic {}", config.base_config.topic);
    let base_consumer = config.make_base_consumer().map_err(|e| format!("{}", e))?;
    let metadata = base_consumer
        .fetch_metadata(
            Some(config.base_config.topic.as_str()),
            Duration::from_millis(1000),
        )
        .map_err(|e| format!("{}", e))?;
    if metadata.topics().is_empty() || metadata.topics()[0].partitions().is_empty() {
        Err(format!(
            "Topic '{}' does not exist",
            config.base_config.topic
        ))
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();

    let min_size = args.min_record_size.unwrap_or(args.max_record_size);
    if let Some(min) = args.min_record_size {
        if args.max_record_size < min {
            error!("Max record size must be >= min record size");
            process::exit(-1);
        }
    }

    let config_builder = ConfigBuilder::new(
        args.brokers,
        args.topic,
        args.username,
        args.password,
        args.sasl_mechanism,
        args.enable_tls,
    )?
    .set_producer_config(
        Payload {
            key_range: args.keys,
            compressible: args.compressible_payload,
            min_size,
            max_size: args.max_record_size,
        },
        args.compression_type,
        args.producer_properties,
        args.produce_throughput_bps,
    )
    .set_consumer_config(
        args.consumer_properties,
        args.consume_throughput_mbps,
        args.rand,
    );

    let config = Arc::new(config_builder.build());

    validate_topic_exists(config.as_ref())?;

    trace!("Creating stats instance");
    let stats = Arc::new(Mutex::new(Stats::new(config.clone())?));

    let stats_handle = StatsHandle::new(stats);

    let cancel_token = CancellationToken::new();

    let mut tasks = vec![];

    let (record_deleter_tx, record_deleter_rx) = mpsc::channel(1);

    info!("Starting monitor water marks");
    tasks.push(tokio::spawn(monitor_water_marks(
        stats_handle.clone(),
        cancel_token.clone(),
        Duration::from_millis(1000),
    )));

    info!("Starting producers");
    tasks.push(tokio::spawn(producers(
        config.clone(),
        args.num_producers,
        Duration::from_millis(args.timeout_ms),
        cancel_token.clone(),
        stats_handle.clone(),
    )));

    info!("Starting record deleter worker");
    tasks.push(tokio::spawn(record_deleter_worker(
        config.clone(),
        stats_handle.clone(),
        cancel_token.clone(),
        record_deleter_rx,
    )));

    info!("Starting record deleter timer");
    tasks.push(tokio::spawn(record_deleter_timer_worker(
        stats_handle.clone(),
        args.delete_record_position,
        args.delete_record_period_sec,
        cancel_token.clone(),
        record_deleter_tx.clone(),
    )));

    info!("Starting webserver");
    tasks.push(tokio::spawn(server(
        cancel_token.clone(),
        args.port,
        stats_handle.clone(),
        record_deleter_tx.clone(),
    )));

    info!("Waiting on exit...");

    let mut tasks_joined = false;

    let mut join_handle = futures::future::join_all(tasks);

    while !tasks_joined {
        select! {
            _ = signal::ctrl_c() => {
                warn!("Received ctrl_c!");
                cancel_token.cancel();
            }
            _ = &mut join_handle => {
                info!("All tasks completed");
                tasks_joined = true;
            }
        }
    }

    info!("Exiting...");

    Ok(())
}
