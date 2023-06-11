use crate::config::Config;
use crate::stats::ErrorReport::Producer;
use crate::stats::StatsHandle;
use governor::{Quota, RateLimiter};
use log::{debug, error, info, warn};
use once_cell::sync::OnceCell;
use rand::RngCore;
use rdkafka::error::KafkaResult;
use rdkafka::producer::FutureRecord;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const MAX_COMPRESSIBLE_PAYLOAD: usize = 128 * 1024 * 1024;

static COMPRESSIBLE_PAYLOAD: OnceCell<[u8; MAX_COMPRESSIBLE_PAYLOAD]> =
    OnceCell::with_value([0x0f; MAX_COMPRESSIBLE_PAYLOAD]);

struct ProducerStats {
    rate: u64,
    total_size: usize,
    errors: Vec<String>,
}

pub async fn producers(
    config: Arc<Config>,
    num_producers: usize,
    timeout: Duration,
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
) {
    let mut tasks = vec![];
    let start_time = Instant::now();
    info!("Spawning {} producers", num_producers);

    for i in 0..num_producers {
        tasks.push(tokio::spawn(produce(
            config.clone(),
            i,
            timeout,
            cancel_token.clone(),
            stats_handle.clone(),
        )))
    }

    let mut results = vec![];
    let mut total_size = 0;
    for (i, t) in tasks.into_iter().enumerate() {
        info!("Joining producer {}...", i);
        match t.await.unwrap() {
            Err(e) => {
                error!("Error during construction of producer {}: {}", i, e)
            }
            Ok(producer_stats) => {
                if !producer_stats.errors.is_empty() {
                    warn!("Producer {} had {} errors", i, producer_stats.errors.len())
                }
                results.push(producer_stats.rate);
                total_size += producer_stats.total_size;
            }
        }
    }

    let total_time = start_time.elapsed();

    if !results.is_empty() {
        let min_result = *results.iter().min().unwrap();
        let max_result = *results.iter().max().unwrap();
        let avg_result = results.iter().sum::<u64>() / results.len() as u64;
        info!(
            "Producer rates: [min={}, max={}, avg={}] bytes/s",
            min_result, max_result, avg_result
        );

        let rate = total_size as u64 / total_time.as_secs();
        // Depending on how many producers are running in parallel
        // the global produce rate could be more or less even if the
        // produce rate for individual producers remains the same.
        info!("Global produce rate: {} bytes/s", rate);
    }

    info!("All producers complete!");
}

async fn produce(
    config: Arc<Config>,
    my_id: usize,
    timeout: Duration,
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
) -> KafkaResult<ProducerStats> {
    debug!("Producer {} constructing", my_id);
    let producer = config.make_future_producer()?;
    let payload = &config.producer_config.payload;

    let mut local_payload: Option<Vec<u8>> = None;
    if !payload.compressible {
        local_payload = Some(vec![0x0f; payload.max_size]);
        rand::thread_rng().fill_bytes(local_payload.as_mut().unwrap());
    }

    debug!("Producer {} sending", my_id);

    let start_time = Instant::now();
    let mut total_size = 0;
    let mut errors = vec![];

    let mut rate_limit = config
        .producer_config
        .producer_throughput_bps
        .map(|mps| RateLimiter::direct(Quota::per_second(mps.try_into().unwrap())));

    let mut running = true;

    while running {
        let key = format!(
            "{:#016x}",
            rand::thread_rng().next_u64() % payload.key_range
        );
        let sz = if payload.min_size != payload.max_size {
            payload.min_size
                + rand::thread_rng().next_u32() as usize % (payload.max_size - payload.min_size)
        } else {
            payload.max_size
        };

        let payload_slice: &[u8] = if payload.compressible {
            &COMPRESSIBLE_PAYLOAD.get().unwrap().as_slice()[0..sz]
        } else {
            &local_payload.as_ref().unwrap()[0..sz]
        };

        total_size += sz;

        if let Some(rl) = &mut rate_limit {
            rl.until_n_ready((sz as u32).try_into().unwrap())
                .await
                .expect("Exceeded rate limiter");
        }
        let fut = producer.send(
            FutureRecord::to(&config.base_config.topic)
                .key(&key)
                .payload(payload_slice),
            timeout,
        );
        debug!("Producer {} waiting", my_id);
        select! {
            _ = cancel_token.cancelled() => {
                info!("Producer {} cancelled", my_id);
                running = false;
            }
            r = fut => {
                match r {
                    Ok(_) => {}
                    Err((e, _)) => {
                        warn!("Error on producer {}, producing {} bytes, compressible={} : {}",
                        my_id, sz, payload.compressible, e);
                        let err_str = format!("{}", e);
                        stats_handle.report_issue(format!("{}", my_id), Producer(err_str.clone()));
                        errors.push(err_str);
                    }
                }
            }
        }
    }

    let total_time = start_time.elapsed();
    let rate = total_size as u64 / total_time.as_secs();

    info!("Producer {} complete with rate {} bytes/s", my_id, rate);

    Ok(ProducerStats {
        rate,
        total_size,
        errors,
    })
}
