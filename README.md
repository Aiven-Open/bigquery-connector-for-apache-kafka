# Kafka Connect BigQuery Connector

This is an implementation of a sink connector from [Apache Kafka] to [Google BigQuery], built on top 
of [Apache Kafka Connect].

## History

This connector was [originally developed by WePay](https://github.com/wepay/kafka-connect-bigquery).
In late 2020 the project moved to [Confluent](https://github.com/confluentinc/kafka-connect-bigquery),
with both companies taking on maintenance duties.
In 2024, Aiven created [its own fork](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/)
based off the Confluent project in order to continue maintaining an open source, Apache 2-licensed
version of the connector.

## Configuration

### Sample

An example connector configuration, that reads records from Kafka with
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
See [here](docs/sink-connector-config-options.rst) for a list of the connector's
configuration properties.

## Download

Releases are available in the GitHub release tab.
<!-- TODO:
  Mention first Aiven-published release (which will be the first to
  include executable artifacts)
-->

  [Apache Kafka Connect]: https://kafka.apache.org/documentation.html#connect
  [Apache Kafka]: http://kafka.apache.org
  [Google BigQuery]: https://cloud.google.com/bigquery/
  [Kafka]: http://kafka.apache.org
  
## Integration test setup

In order to execute the integration specific environment variables must be set.

### Local configuration

GOOGLE_APPLICATION_CREDENTIALS - the path to a json file that was download when the GCP account key was created..

KCBQ_TEST_BUCKET - the name of the bucket to use for testing,

KCBQ_TEST_DATASET - the name of the dataset to use for testing,

KCBQ_TEST_KEYFILE - same as the GOOGLE_APPLICATION_CREDENTIALS

KCBQ_TEST_PROJECT - the name of the project to use. 

### Github configuration

GCP_CREDENTIALS - the contents of a json file that was download when the GCP account key was created.

KCBQ_TEST_BUCKET - the bucket to use for the tests

KCBQ_TEST_DATASET - the data set to use for the tests.

KCBQ_TEST_PROJECT - the project to use for the tests.

## Building documentation

The documentation is not built as part of the standard build.  To build the documentation execute the following steps.

    mvn install -DskipITs
    mvn -f tools/
    mvn -f docs/
    
    # To run the docs locally
    mvn -f docs/ site:run


