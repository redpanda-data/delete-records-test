mod config;

use crate::config::{CompressionType, Config, DeleteOffsetPosition};
use clap::Parser;
use log::debug;
use rdkafka::consumer::Consumer;
use std::num;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, help = "Brokers")]
    brokers: String,
    #[arg(short, long, help = "Existing topic name")]
    topic: String,
    #[arg(short, long, help = "HTTP port", value_parser = port_in_range, default_value_t = 7778)]
    port: u16,
    #[arg(long, help = "Generate compressible payload")]
    compressible_payload: bool,
    #[arg(long, help = "Compression Type", value_enum)]
    compression_type: Option<CompressionType>,
    #[arg(long, help = "Position to delete offset", value_enum)]
    delete_offset_position: DeleteOffsetPosition,
    #[arg(long, help = "Frequency of running delete offset in seconds", value_parser = parse_duration)]
    delete_offset_period_sec: Duration,
    #[arg(long, help = "Produce throughput in bps")]
    produce_throughput_bps: Option<usize>,
    #[arg(long, help = "Consume throughput in MBps")]
    consume_throughput_mbps: Option<usize>,
    #[arg(long, help = "Producer properties (as key=value for librdkafka)")]
    producer_properties: Vec<String>,
    #[arg(long, help = "Consumer properties (as key=value for librdkafka)")]
    consumer_properties: Vec<String>,
    #[arg(long, help = "Number of consumers", default_value_t = 1)]
    num_consumers: usize,
    #[arg(long, help = "Random consumption rather than sequential")]
    rand: bool,
    #[arg(long, help = "Username")]
    username: Option<String>,
    #[arg(long, help = "Password")]
    password: Option<String>,
    #[arg(long)]
    sasl_mechanism: Option<String>,
    #[arg(long)]
    enable_tls: bool,
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
    debug!("Fetching metadata for topic {}", config.topic);
    let base_consumer = config.make_base_consumer().map_err(|e| format!("{}", e))?;
    let metadata = base_consumer
        .fetch_metadata(Some(config.topic.as_str()), Duration::from_millis(1000))
        .map_err(|e| format!("{}", e))?;
    if metadata.topics().is_empty() || metadata.topics()[0].partitions().is_empty() {
        Err(format!("Topic '{}' does not exist", config.topic))
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();

    let config = Arc::new(Config::new(
        args.brokers,
        args.topic,
        args.username,
        args.password,
        args.sasl_mechanism,
        args.enable_tls,
    )?);

    validate_topic_exists(config.as_ref())?;

    Ok(())
}
