use log::{info, warn};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use schema_registry_converter::async_impl::json::JsonDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Header;
use rdkafka::message::OwnedHeaders;
use rdkafka::message::OwnedMessage;
use rdkafka::message::ToBytes;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Message;

use crate::error::Error as RestError;
use crate::Args;

type Result<T> = std::result::Result<T, RestError>;

#[derive(Clone)]
pub struct CustomKafkaClient<'a> {
    pub consumer: Arc<LoggingConsumer>,
    pub producer: Arc<FutureProducer>,
    pub decoder: Arc<JsonDecoder<'a>>,
}

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("\"Pre-rebalance {:?}\"", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("\"Post-rebalance {:?}\"", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => log::debug!("\"Offsets committed successfully\""),
            Err(e) => log::warn!("\"Error while committing offsets: {}\"", e),
        };
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

pub async fn decode(
    decoder: Arc<JsonDecoder<'_>>,
    message: Option<&[u8]>,
) -> Result<Option<Value>> {
    // Decode message
    match decoder.decode(message).await {
        Ok(v) => match v {
            Some(value) => Ok(Some(value.value)),
            None => {
                log::debug!("\"Got empty message payload\"");
                Ok(None)
            }
        },
        Err(e) => {
            warn!("\"Error while deserializing message payload: {:?}\"", e);
            Err(e)?
        }
    }
}

pub async fn dlq_produce(
    producer: Arc<FutureProducer>,
    mut message: OwnedMessage,
    topic: &str,
    error: String,
) {
    // Own headers
    let headers_detached = match message.detach_headers() {
        Some(h) => h,
        None => OwnedHeaders::new(),
    };

    // Inject error message into headers
    let headers = headers_detached.clone().insert(Header {
        key: "error",
        value: Some(&error),
    });

    // Convert message payload
    let payload = match message.payload() {
        Some(p) => p.to_bytes(),
        None => b"",
    };

    // Get message key
    let key = match message.key() {
        Some(p) => p.to_bytes(),
        None => b"",
    };

    // Send message to kafka async
    let delivery_status = producer
        .send(
            FutureRecord::to(topic)
                .payload(payload)
                .key(key)
                .headers(headers),
            Duration::from_secs(0),
        )
        .await;

    // This will be executed when the result is received.
    match delivery_status {
        Ok(_) => log::warn!("\"Saved message to dlq: {}\"", topic),
        Err(e) => log::warn!("\"Unable to commit message to dlq! {}\"", e.0),
    }
}

impl<'a> CustomKafkaClient<'a> {
    pub fn init(args: Args) -> Result<CustomKafkaClient<'a>> {
        // It probably is not necessary to include consumer and producer
        Ok(Self {
            consumer: Arc::new(CustomKafkaClient::consumer(&args)?),
            producer: Arc::new(CustomKafkaClient::producer(&args)?),
            decoder: Arc::new(CustomKafkaClient::decoder(args.clone())?),
        })
    }

    pub fn decoder(args: Args) -> Result<JsonDecoder<'a>> {
        let srsettings = SrSettings::new_builder(String::from(&args.schema_registry_endpoint))
            .set_basic_authorization(
                &args.schema_registry_api_key,
                Some(&args.schema_registry_api_secret),
            )
            .build()
            .unwrap();

        let decoder = JsonDecoder::new(srsettings);
        Ok(decoder)
    }

    pub fn producer(args: &Args) -> Result<FutureProducer> {
        let producer: FutureProducer = ClientConfig::new()
            .set("message.timeout.ms", "5000")
            .set("bootstrap.servers", args.broker_endpoint.clone())
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", "PLAIN")
            .set("sasl.username", &args.kafka_username)
            .set("sasl.password", &args.kafka_password)
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Producer creation failed");

        Ok(producer)
    }

    pub fn consumer(args: &Args) -> Result<LoggingConsumer> {
        let context = CustomContext;

        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", args.group_id.clone())
            .set("bootstrap.servers", args.broker_endpoint.clone())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000") // Commit automatically every 5 seconds.
            .set("enable.auto.offset.store", "false") // only commit the offsets explicitly stored via `consumer.store_offset`.
            .set("auto.offset.reset", "earliest")
            .set("security.protocol", &args.kafka_security_protocol)
            .set("sasl.mechanisms", &args.kafka_sasl_mechanism)
            .set("sasl.username", &args.kafka_username)
            .set("sasl.password", &args.kafka_password)
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        Ok(consumer)
    }
}
