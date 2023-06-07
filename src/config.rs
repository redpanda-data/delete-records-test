use log::info;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaResult;
use rdkafka::ClientConfig;

pub struct Config {
    pub brokers: String,
    pub topic: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub enable_tls: bool,
    rdkafka_config: ClientConfig,
}

impl Config {
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

        Ok(Self {
            brokers,
            topic,
            username,
            password,
            sasl_mechanism,
            enable_tls,
            rdkafka_config,
        })
    }

    pub fn make_admin(&self) -> KafkaResult<rdkafka::admin::AdminClient<DefaultClientContext>> {
        self.rdkafka_config.create()
    }

    pub fn make_base_consumer(&self) -> KafkaResult<rdkafka::consumer::BaseConsumer> {
        self.rdkafka_config.create()
    }
}
