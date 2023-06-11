use crate::config::Config;
use crate::stats::{ErrorReport, StatsHandle};
use futures::TryStreamExt;
use governor::{Quota, RateLimiter};
use log::{debug, error, info, warn};
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaResult;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio_util::sync::CancellationToken;

struct ConsumerStats {
    rate: u64,
    total_size: usize,
    errors: Vec<String>,
}

pub async fn consumers(
    config: Arc<Config>,
    num_consumers: usize,
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
) {
    let mut tasks = vec![];
    let start_time = Instant::now();
    info!("Spawning {} consumers", num_consumers);

    for i in 0..num_consumers {
        tasks.push(tokio::spawn(consumer(
            config.clone(),
            i,
            cancel_token.clone(),
            stats_handle.clone(),
        )));
    }

    let mut results = vec![];
    let mut total_size = 0;

    for (i, t) in tasks.into_iter().enumerate() {
        info!("Joining consumer {}...", i);
        match t.await.unwrap() {
            Err(e) => {
                error!("Error during construction of consumer {}: {}", i, e);
            }
            Ok(stats) => {
                if !stats.errors.is_empty() {
                    warn!("Consumer {} had {} errors", i, stats.errors.len());
                }
                results.push(stats.rate);
                total_size += stats.total_size;
            }
        }
    }

    let total_time = start_time.elapsed();

    if !results.is_empty() {
        let min_result = *results.iter().min().unwrap();
        let max_result = *results.iter().max().unwrap();
        let avg_result = results.iter().sum::<u64>() / results.len() as u64;
        info!(
            "Consumer rates: [min={}, max={}, avg={}] bytes/s",
            min_result, max_result, avg_result
        );

        let rate = total_size as u64 / total_time.as_secs();
        // Depending on how many producers are running in parallel
        // the global produce rate could be more or less even if the
        // produce rate for individual producers remains the same.
        info!("Global consumer rate: {} bytes/s", rate);
    }

    info!("All consumer complete!");
}

async fn consumer(
    config: Arc<Config>,
    my_id: usize,
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
) -> KafkaResult<ConsumerStats> {
    let consumer = config.make_stream_consumer()?;

    debug!("Consumer {} starting", my_id);

    let start_time = Instant::now();
    let mut total_size = 0;
    let mut errors = vec![];
    consumer.subscribe(&[config.base_config.topic.as_str()])?;

    let mut stream = consumer.stream();

    let mut running = true;

    let bps = config
        .consumer_config
        .consumer_throughput_mbps
        .map(|mbps| mbps * 1024 * 1024);

    let mut rate_limit =
        bps.map(|mps| RateLimiter::direct(Quota::per_second(mps.try_into().unwrap())));

    while running {
        select! {
            _ = cancel_token.cancelled() => {
                warn!("Consumer {} cancelled", my_id);
                running = false;
            }
            item = stream.try_next() => {
                match item {
                    Err(e) => {
                        stats_handle.report_issue(format!("Consumer {}", my_id), ErrorReport::Consumer(format!("{}", e)));
                        errors.push(format!("{}", e));
                        warn!("Error with consumer {}: {}", my_id, e);
                    }
                    Ok(msg) => {
                        if let Some(msg) = msg {
                            total_size += msg.payload_len();
                            if let Some(rl) = &mut rate_limit {
                                if rl.until_n_ready((msg.payload_len() as u32).try_into().unwrap())
                                .await.is_err() {
                                    warn!("Received message larger than permitted size");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let total_time = start_time.elapsed();
    let rate = total_size as u64 / total_time.as_secs();

    info!("Consumer {} completed with rate {} bytes/s", my_id, rate);

    Ok(ConsumerStats {
        rate,
        total_size,
        errors,
    })
}
