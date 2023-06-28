use crate::config::Config;
use crate::stats::{ErrorReport, StatsHandle};
use futures::TryStreamExt;
use governor::{Quota, RateLimiter};
use log::{debug, error, info, trace, warn};
use rand::Rng;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
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
    enable_random_consumers: bool,
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

    if enable_random_consumers {
        info!("Spawning {} random consumers", num_consumers);
        for i in 0..num_consumers {
            tasks.push(tokio::spawn(random_consumer(
                config.clone(),
                i,
                cancel_token.clone(),
                stats_handle.clone(),
            )))
        }
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
                            if msg.offset() % 1000 == 0 {
                                debug!("Consumer {} read {}/{}:{}", my_id, msg.topic(), msg.partition(), msg.offset());
                            }
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

async fn random_consumer(
    config: Arc<Config>,
    my_id: usize,
    cancel_token: CancellationToken,
    stats_handle: StatsHandle,
) -> KafkaResult<ConsumerStats> {
    let metadata = config.make_base_consumer()?.fetch_metadata(
        Some(config.base_config.topic.as_str()),
        Timeout::After(Duration::from_secs(5)),
    )?;

    if metadata.topics().is_empty() || metadata.topics()[0].name() != config.base_config.topic {
        panic!(
            "Failed to fetch metadata for topic {}",
            config.base_config.topic
        );
    }

    let num_partitions = metadata.topics()[0].partitions().len();

    if num_partitions == 0 {
        panic!("Topic {} has zero partitions", config.base_config.topic);
    }

    debug!("Random consumer {} starting", my_id);

    let start_time = Instant::now();
    let mut total_size = 0;
    let mut errors = vec![];

    let mut running = true;

    let bps = config
        .consumer_config
        .consumer_throughput_mbps
        .map(|mbps| mbps * 1024 * 1024);

    let mut rate_limit =
        bps.map(|mps| RateLimiter::direct(Quota::per_second(mps.try_into().unwrap())));

    while running {
        let consumer = config
            .make_random_consumer()
            .expect("Failed to create random consumer");
        let partition = rand::thread_rng().gen_range(0..num_partitions) as i32;
        let (offset, is_deleted) = get_offset(
            &config.make_base_consumer()?,
            config.base_config.topic.as_str(),
            partition,
            &stats_handle,
            0.1,
        );
        let offset = Offset::Offset(offset);

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(config.base_config.topic.as_str(), partition, offset)
            .expect("Failed to create partition offset");

        consumer.assign(&tpl)?;

        consumer.subscribe(&[config.base_config.topic.as_str()])?;
        consumer.seek(
            config.base_config.topic.as_str(),
            partition,
            offset,
            Timeout::After(Duration::from_secs(5)),
        )?;

        let mut stream = consumer.stream();

        let msg = stream.try_next().await;

        select! {
                    _ = cancel_token.cancelled() => {
                        warn!("Random Consumer {} cancelled", my_id);
                        running = false
                    }
                    item = tokio::time::timeout(Duration::from_secs(5), stream.try_next()) => {
                        if let Ok(result) = item {
        match result {
                        Err(e) => {
                            stats_handle.report_issue(
                                format!("Random consumer {}", my_id),
                                ErrorReport::Consumer(format!("{}", e)),
                            );
                            errors.push(format!("{}", e));
                            warn!("Error with random consumer {}: {}", my_id, e);
                        }
                        Ok(Some(m)) => {
                            // See note below about rdkafka seek not being helpful
                            if is_deleted && m.partition() == partition && Offset::Offset(m.offset()) == offset {
                                let msg = format!(
                                    "Random consumer {} received expected deleted offset {}/{}:{:?}",
                                    my_id, config.base_config.topic, partition, offset
                                );
                                stats_handle.report_issue(
                                    format!("Random consumer {}", my_id),
                                    ErrorReport::Consumer(msg.clone()),
                                );
                                errors.push(msg.clone());
                                error!("{}", msg);
                            } else if m.partition() != partition {
                                // Because rdkafka does not provide any feedback from the above
                                // seek call if it succeeds or not, we are at the mercy of the
                                // commit reset which may reset us to a record that is not on the
                                // partition we are expecting.  So we need to be tolerant of that
                                warn!("Random consumer {} received message from unexpected partition: Expected ({}/{:?}), Received ({}/{:?})", my_id, partition, offset,  m.partition(), m.offset());
                            }

                            total_size += m.payload_len();

                            if let Some(rl) = &mut rate_limit {
                                if rl.until_n_ready((m.payload_len() as u32).try_into().unwrap()).await.is_err() {
                                    warn!("Received message larger than permitted size");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                        Ok(None) => {}
                    }
                } else if !is_deleted {
                    warn!(
                        "Random consumer {} did not read expected present offset {}/{}:{:?}",
                        my_id, config.base_config.topic, partition, offset
                    );
                }
                    }
                }
    }

    let total_time = start_time.elapsed();
    let rate = total_size as u64 / total_time.as_secs();

    info!(
        "Random Consumer {} completed with rate {} bytes/s",
        my_id, rate
    );

    Ok(ConsumerStats {
        rate,
        total_size,
        errors,
    })
}

fn get_offset(
    base_consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    stats_handle: &StatsHandle,
    select_deleted_rate: f32,
) -> (crate::stats::Offset, bool) {
    let partition = rand::thread_rng().gen_range(0..=partition);

    let (lwm, hwm) = base_consumer
        .fetch_watermarks(topic, partition, Timeout::After(Duration::from_secs(5)))
        .expect("Failed to fetch watermarks");

    let last_removed_offsets = stats_handle.last_removed_offsets();

    let mut use_deleted = select_deleted_rate >= rand::thread_rng().gen_range(0.0..1.0)
        && last_removed_offsets.is_some();

    let offset_to_delete = if use_deleted
        && last_removed_offsets
            .as_ref()
            .unwrap()
            .get(topic)
            .unwrap()
            .get(&partition)
            .is_some()
    {
        let last_removed_offset = *(last_removed_offsets
            .unwrap()
            .get(topic)
            .unwrap()
            .get(&partition)
            .unwrap());

        rand::thread_rng().gen_range(0..last_removed_offset)
    } else {
        use_deleted = false;
        rand::thread_rng().gen_range(lwm..=hwm)
    };

    debug!(
        "Selected {}/{}:{} (deleted={})",
        topic, partition, offset_to_delete, use_deleted
    );

    (offset_to_delete, use_deleted)
}
