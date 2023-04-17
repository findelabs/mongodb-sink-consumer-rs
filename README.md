# mongodb-sink-consumer-rs

Kafka consumer whose sole purpose is to sink messages into a MongoDB collection.

### Usage

```
Options:
  -b, --broker-endpoint <BROKER_ENDPOINT>
          Kafka Brokers [env: KAFKA_BROKER_ENDPOINT=]
  -t, --topic <TOPIC>
          Kafka main topic [env: KAFKA_TOPIC=]
  -t, --topic-dlq <TOPIC_DLQ>
          Kafka dlq topic [env: KAFKA_TOPIC_DLQ=]
  -g, --group-id <GROUP_ID>
          Kafka group id [env: KAFKA_GROUP_ID=]
  -k, --kafka-username <KAFKA_USERNAME>
          Kafka username [env: KAFKA_BROKER_USERNAME=]
  -k, --kafka-password <KAFKA_PASSWORD>
          Kafka password [env: KAFKA_BROKER_PASSWORD=]
  -k, --kafka-security-protocol <KAFKA_SECURITY_PROTOCOL>
          Kafka security protocol [env: KAFKA_BROKER_SECURITY_PROTOCOL=]
  -k, --kafka-sasl-mechanism <KAFKA_SASL_MECHANISM>
          Kafka SASL mechanism [env: KAFKA_BROKER_SASL_MECHANISM=]
  -s, --schema-registry-endpoint <SCHEMA_REGISTRY_ENDPOINT>
          Kafka schema endpoint [env: KAFKA_SCHEMA_REGISTRY_ENDPOINT=]
  -s, --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY>
          Kafka schema username [env: KAFKA_SCHEMA_REGISTRY_API_KEY=]
  -s, --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
          Kafka schema password [env: KAFKA_SCHEMA_REGISTRY_API_SECRET=]
  -m, --mongodb-username <MONGODB_USERNAME>
          MongoDB Username [env: MONGODB_USERNAME=]
  -m, --mongodb-password <MONGODB_PASSWORD>
          MongoDB Password [env: MONGODB_PASSWORD=]
  -m, --mongodb-uri <MONGODB_URI>
          MongoDB URI [env: MONGODB_URI=]
  -m, --mongodb-database <MONGODB_DATABASE>
          MongoDB Database [env: MONGODB_DATABASE=]
  -m, --mongodb-collection <MONGODB_COLLECTION>
          MongoDB Collection [env: MONGODB_COLLECTION=]
  -h, --help
          Print help
  -V, --version
          Print version
```
