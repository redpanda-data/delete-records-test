use clap::ValueEnum;
use log::info;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaResult;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use serde::Deserialize;
use std::fmt;
use std::fmt::Formatter;

#[derive(Copy, Clone, PartialOrd, PartialEq, Eq, Ord, ValueEnum)]
pub enum CompressionType {
    None,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Zstd => write!(f, "zstd"),
        }
    }
}

#[derive(Copy, Clone, PartialOrd, PartialEq, Ord, Eq, ValueEnum, Deserialize, Debug)]
pub enum DeleteRecordPosition {
    /// Delete all offsets
    All,
    /// Delete halfway between HWM and LWM
    Halfway,
    /// Delete one after LWM (single offset deletion)
    Single,
}
impl fmt::Display for DeleteRecordPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DeleteRecordPosition::All => write!(f, "All"),
            DeleteRecordPosition::Halfway => write!(f, "Halfway"),
            DeleteRecordPosition::Single => write!(f, "Single"),
        }
    }
}

pub struct BaseConfig {
    pub brokers: String,
    pub topic: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub enable_tls: bool,
    rdkafka_config: ClientConfig,
}

#[derive(Clone)]
pub struct Payload {
    pub key_range: u64,
    pub compressible: bool,
    pub min_size: usize,
    pub max_size: usize,
}

pub struct ProducerConfig {
    pub payload: Payload,
    pub producer_throughput_bps: Option<u32>,
    compression_type: Option<CompressionType>,
    producer_properties: Vec<String>,
}

pub struct ConsumerConfig {
    consumer_properties: Vec<String>,
    pub consumer_throughput_mbps: Option<usize>,
    pub rand: bool,
}

pub struct ConfigBuilder {
    base_config: Option<BaseConfig>,
    producer_config: Option<ProducerConfig>,
    consumer_config: Option<ConsumerConfig>,
}

impl ConfigBuilder {
    pub fn new(
        brokers: String,
        topic: String,
        username: Option<String>,
        password: Option<String>,
        sasl_mechanism: Option<String>,
        enable_tls: bool,
    ) -> Result<Self, String> {
        let mut rdkafka_config = ClientConfig::new();
        rdkafka_config.set("bootstrap.servers", &brokers);

        if let Some(username) = &username {
            info!("Using SASL/SSL auth for user {}", username);
            rdkafka_config.set(
                "security.protocol",
                if enable_tls { "sasl_ssl" } else { "sasl" },
            );
            rdkafka_config.set("sasl.username", username);
            if sasl_mechanism.is_none() {
                return Err(String::from("Username set but sasl mechanism is not set"));
            }
            if password.is_none() {
                return Err(String::from("Username set by password not set"));
            }
        } else {
            info!("Using anonymous auth (no username)");
        }

        if let Some(password) = &password {
            rdkafka_config.set("sasl.password", password);
        }

        if let Some(mechanism) = &sasl_mechanism {
            rdkafka_config.set("sasl.mechanism", mechanism);
        }

        let base_config = BaseConfig {
            brokers,
            topic,
            username,
            password,
            sasl_mechanism,
            enable_tls,
            rdkafka_config,
        };

        Ok(Self {
            base_config: Some(base_config),
            producer_config: None,
            consumer_config: None,
        })
    }

    pub fn set_producer_config(
        mut self,
        payload: Payload,
        compression_type: Option<CompressionType>,
        producer_properties: Vec<String>,
        producer_throughput_bps: Option<u32>,
    ) -> Self {
        self.producer_config = Some(ProducerConfig {
            payload,
            producer_throughput_bps,
            compression_type,
            producer_properties,
        });
        self
    }

    pub fn set_consumer_config(
        mut self,
        consumer_properties: Vec<String>,
        consumer_throughput_mbps: Option<usize>,
        rand: bool,
    ) -> Self {
        self.consumer_config = Some(ConsumerConfig {
            consumer_properties,
            consumer_throughput_mbps,
            rand,
        });
        self
    }

    pub fn build(self) -> Config {
        Config {
            base_config: self.base_config.expect("Base config not provided"),
            producer_config: self.producer_config.expect("Producer config not provided"),
            consumer_config: self.consumer_config.expect("Consumer config not provided"),
        }
    }
}

pub struct Config {
    pub base_config: BaseConfig,
    pub producer_config: ProducerConfig,
    pub consumer_config: ConsumerConfig,
}

impl Config {
    pub fn make_admin(&self) -> KafkaResult<rdkafka::admin::AdminClient<DefaultClientContext>> {
        self.base_config.rdkafka_config.create()
    }

    pub fn make_base_consumer(&self) -> KafkaResult<rdkafka::consumer::BaseConsumer> {
        self.base_config.rdkafka_config.create()
    }

    fn split_properties(properties: &[String]) -> Vec<(String, String)> {
        properties
            .iter()
            .map(|p| p.split('=').collect::<Vec<&str>>())
            .map(|parts| (parts[0].to_string(), parts[1].to_string()))
            .collect()
    }

    pub fn make_future_producer(&self) -> KafkaResult<FutureProducer> {
        let mut cfg = self.base_config.rdkafka_config.clone();
        cfg.set("message.max.bytes", "1000000000");
        let kv_pairs = Self::split_properties(&self.producer_config.producer_properties);
        for (k, v) in kv_pairs {
            cfg.set(k, v);
        }
        if let Some(compression_type) = self.producer_config.compression_type {
            cfg.set("compression.type", format!("{}", compression_type));
        }

        cfg.create()
    }
}
