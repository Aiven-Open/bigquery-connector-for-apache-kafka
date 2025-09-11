# Kafka Connect BigQuery Connector

This is an implementation of a sink connector from [Apache Kafka] to [Google BigQuery], built on top 
of [Apache Kafka Connect].

## Documentation

The Kafka Connect BigQuery Connector documentation is available online at https://aiven-open.github.io/bigquery-connector-for-apache-kafka/.
The site contains a complete list of the configuration options as well as information about the project.

## History

This connector was [originally developed by WePay](https://github.com/wepay/kafka-connect-bigquery).
In late 2020 the project moved to [Confluent](https://github.com/confluentinc/kafka-connect-bigquery),
with both companies taking on maintenance duties.
In 2024, Aiven created [its own fork](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/)
based off the Confluent project in order to continue maintaining an open source, Apache 2-licensed
version of the connector.

## Configuration

### Sample

A simple example connector configuration, that reads records from Kafka with
JSON-encoded values and writes their values to BigQuery:

```json
{
  "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
  "topics": "users, clicks, payments",
  "tasks.max": "3",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",

  "project": "kafka-ingest-testing",
  "defaultDataset": "kcbq-example",
  "keyfile": "/tmp/bigquery-credentials.json"
}
```

### Complete docs
See the [configuration documentation](https://aiven-open.github.io/bigquery-connector-for-apache-kafka/configuration.html) for a list of the connector's
configuration properties.

## Download

Download information is available on the [project web site]((https://aiven-open.github.io/bigquery-connector-for-apache-kafka)). 

## Building from source

This project uses the Maven build tool.

To compile the project without running the integration tests execute `mvn package -DskipITs`.

To build the documentation execute the following steps:

```
mvn install -DskipIts
mvn -f tools
mvn -f docs
```

Once the documentation is built it can be run by executing `mvn -f docs site:run`.

### Integration test setup

Integration tests require a live BigQuery and Kafka installation.  Configuring those components is beyond the scope of this document.

Once you have the test environment ready, integration specific environment variables must be set.

#### Local configuration

- GOOGLE_APPLICATION_CREDENTIALS - the path to a json file that was download when the GCP account key was created.
- KCBQ_TEST_BUCKET - the name of the bucket to use for testing,
- KCBQ_TEST_DATASET - the name of the dataset to use for testing,
- KCBQ_TEST_KEYFILE - same as the GOOGLE_APPLICATION_CREDENTIALS
- KCBQ_TEST_PROJECT - the name of the project to use.  

#### GitHub configuration

To run the integration tests from a GitHub action the following variables must be set

- GCP_CREDENTIALS - the contents of a json file that was download when the GCP account key was created.
- KCBQ_TEST_BUCKET - the bucket to use for the tests
- KCBQ_TEST_DATASET - the data set to use for the tests.
- KCBQ_TEST_PROJECT - the project to use for the tests.
