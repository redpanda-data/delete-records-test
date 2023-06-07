mod config;

use crate::config::Config;
use clap::Parser;
use log::debug;
use rdkafka::consumer::Consumer;
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
    #[arg(long, help = "Produce throughput in bps")]
    produce_throughput_bps: Option<usize>,
    #[arg(long, help = "Consume throughput in MBps")]
    consume_throughput_mbps: Option<usize>,
    #[arg(long, help = "Number of consumers", default_value_t = 1)]
    num_consumers: usize,
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

    let base_consumer = config.make_base_consumer()?;

    debug!("Fetching metadata for topic {}", config.topic);
    let metadata =
        base_consumer.fetch_metadata(Some(config.topic.as_str()), Duration::from_millis(1000))?;

    if metadata.topics()[0].partitions().is_empty() {
        panic!("Topic `{}` does not exist", config.topic);
    }

    debug!(
        "Topic `{}` contains {} partitions",
        config.topic,
        metadata.topics()[0].partitions().len()
    );

    Ok(())
}
