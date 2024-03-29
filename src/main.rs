use axum::routing::get;
use axum::Router;
use bson::to_document;
use chrono::Local;
use clap::Parser;
use env_logger::{Builder, Target};
use log::LevelFilter;
use std::future::ready;
use std::io::Write;
use std::net::SocketAddr;
use tokio::task;

use rdkafka::consumer::Consumer;
use rdkafka::message::{Headers, Message};
use rdkafka::util::get_rdkafka_version;

mod error;
mod exporter;
mod kafka;
mod mongo;

use crate::error::Error as RestError;
use crate::kafka::CustomKafkaClient;
use crate::mongo::MongoClient;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Port to listen on
    #[arg(short = 'l', long, default_value_t = 8080, env = "METRICS_PORT")]
    port: u16,

    /// Kafka Brokers
    #[arg(short = 'b', long, env = "KAFKA_BROKER_ENDPOINT")]
    broker_endpoint: String,

    /// Kafka main topic
    #[arg(short = 't', long, env = "KAFKA_TOPIC")]
    topic: String,

    /// Kafka dlq topic
    #[arg(short = 'd', long, env = "KAFKA_TOPIC_DLQ")]
    topic_dlq: String,

    /// Kafka group id
    #[arg(short = 'g', long, env = "KAFKA_GROUP_ID")]
    group_id: String,

    /// Kafka username
    #[arg(short = 'k', long, env = "KAFKA_BROKER_USERNAME")]
    kafka_username: String,

    /// Kafka password
    #[arg(short = 'p', long, env = "KAFKA_BROKER_PASSWORD")]
    kafka_password: String,

    /// Kafka security protocol
    #[arg(short = 'S', long, env = "KAFKA_BROKER_SECURITY_PROTOCOL")]
    kafka_security_protocol: String,

    /// Kafka SASL mechanism
    #[arg(short = 'M', long, env = "KAFKA_BROKER_SASL_MECHANISM")]
    kafka_sasl_mechanism: String,

    /// Kafka schema endpoint
    #[arg(short = 'r', long, env = "KAFKA_SCHEMA_REGISTRY_ENDPOINT")]
    schema_registry_endpoint: String,

    /// Kafka schema username
    #[arg(short = 'a', long, env = "KAFKA_SCHEMA_REGISTRY_API_KEY")]
    schema_registry_api_key: String,

    /// Kafka schema password
    #[arg(short = 's', long, env = "KAFKA_SCHEMA_REGISTRY_API_SECRET")]
    schema_registry_api_secret: String,

    /// MongoDB Username
    #[arg(short = 'U', long, env = "MONGODB_USERNAME")]
    mongodb_username: String,

    /// MongoDB Password
    #[arg(short = 'P', long, env = "MONGODB_PASSWORD")]
    mongodb_password: String,

    /// MongoDB URI
    #[arg(short = 'R', long, env = "MONGODB_URI")]
    mongodb_uri: String,

    /// MongoDB Database
    #[arg(short = 'm', long, env = "MONGODB_DATABASE")]
    mongodb_database: String,

    /// MongoDB Collection
    #[arg(short = 'c', long, env = "MONGODB_COLLECTION")]
    mongodb_collection: String,
}

async fn consume(
    client: CustomKafkaClient<'_>,
    db: MongoClient,
    dlq: String,
) -> Result<(), RestError> {
    loop {
        match client.consumer.recv().await {
            Err(e) => log::warn!("Kafka error: {}", e),
            Ok(m) => {
                metrics::increment_counter!("mongodb_sink_connector_topic_message_recv");

                // Convert message from JSON with schema registry
                let payload = match kafka::decode(client.decoder.clone(), m.payload()).await {
                    Ok(p) => p,
                    Err(e) => {
                        log::debug!("Unable to decode message: {}", e);
                        kafka::dlq_produce(
                            client.producer.clone(),
                            m.detach(),
                            &dlq,
                            e.to_string(),
                        )
                        .await;
                        continue;
                    }
                };

                // Debug the message we received
                log::debug!("key: '{:?}', payload: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                // Output any headers if we want extra debugging
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        log::debug!("Header {:#?}: {:?}", header.key, header.value);
                    }
                }

                // Commit to mongo if there is a message
                if let Some(doc) = payload {
                    let mut doc = to_document(&doc)?;

                    // Inject message and created timestamps
                    if let Some(timestamp) = m.timestamp().to_millis() {
                        let datetime = bson::DateTime::from_millis(timestamp);
                        doc.insert("_messageTS", datetime);
                        doc.insert("_createdTS", bson::DateTime::now());
                    }

                    // Insert document into MongoDB
                    match db.insert_one(doc).await {
                        Ok(_) => {
                            // Commit to the offset store. Offsets are committed back to kafka at regular intervals
                            if let Err(e) = client.consumer.store_offset_from_message(&m) {
                                log::warn!("\"Error while storing offset: {}\"", e);
                                metrics::increment_counter!(
                                    "mongodb_sink_connector_offsets_stored_errors"
                                );
                            } else {
                                metrics::increment_counter!(
                                    "mongodb_sink_connector_offsets_stored"
                                );
                            }
                        }
                        Err(e) => {
                            log::debug!("\"Caught error saving to mongo: {}\"", e);
                            kafka::dlq_produce(
                                client.producer.clone(),
                                m.detach(),
                                &dlq,
                                e.to_string(),
                            )
                            .await;
                            continue;
                        }
                    }
                }
            }
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), RestError> {
    let args = Args::parse();

    // Initialize log Builder
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{{\"date\": \"{}\", \"level\": \"{}\", \"log\": {}}}",
                Local::now().format("%Y-%m-%dT%H:%M:%S:%f"),
                record.level(),
                record.args()
            )
        })
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    // Create prometheus handle
    let recorder_handle = exporter::setup_metrics_recorder();

    let (version_n, version_s) = get_rdkafka_version();
    log::info!("\"rd_kafka_version: 0x{:08x}, {}\"", version_n, version_s);

    // Create new kafka client
    let kafka_client = CustomKafkaClient::init(args.clone())?;

    // Create new mongo client
    let db = MongoClient::init(args.clone()).await?;

    // Subscribe to kafka topic
    kafka_client
        .consumer
        .subscribe(&[&args.topic])
        .expect("Can't subscribe to specified topic");

    // Create listener for /metrics
    let app = Router::new().route("/metrics", get(move || ready(recorder_handle.render())));

    // Serve metrics
    let addr = SocketAddr::from(([0, 0, 0, 0], args.port as u16));
    log::info!("\"Listening on {}\"", addr);
    task::spawn(async move {
        if let Err(e) = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
        {
            log::error!("\"Caught metrics error on listener: {}\"", e);
        };
    });

    // Consume from topic
    consume(kafka_client, db, args.topic_dlq).await
}
