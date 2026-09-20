# Kafka Connect BigQuery Connector


[![Build site and deploy](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/actions/workflows/build_site.yml/badge.svg)](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/actions/workflows/build_site.yml)

This is an implementation of a sink connector from [Apache Kafka](http://kafka.apache.org) to 
[Google BigQuery](https://cloud.google.com/bigquery/), built on top 
of [Apache Kafka Connect](https://kafka.apache.org/documentation.html#connect).

## Documentation

The Kafka Connect BigQuery Connector documentation is available online at https://aiven-open.github.io/bigquery-connector-for-apache-kafka/.
The site contains a complete list of the configuration options as well as information about the project.

### Configuration notes

If the configuration includes a JSON GCP credential structure that uses a `credential_source` entry, one of the following environment variables must be set. This does not apply to `keySource=WIF_JSON`, which strips `credential_source` before validation — see [Workload Identity Federation (WIF_JSON)](#workload-identity-federation-wif_json).

| Source Type | Environment Variable            |
|-------------|---------------------------------|
| file        | io.aiven.commons.envcheck.files |
| url         | io.aiven.commons.envcheck.uri   |
| executable  | io.aiven.commons.envcheck.cmd   |

The environment variables contain a comma separated list of valid entries for each type.  If the environment variable is not set, or the JSON value is not found in the environment variable, the value will be prohibited and an exception thrown before the connector starts. 

As an example, to access https://example.com/credentials.cgi the environment variable `io.aiven.commons.envcheck.uri` would need to contain the URL:

```
export io.aiven.commons.envcheck.uri=https://example.com/credentials.cgi
# start the kafka processes
```

To add an additional URL, for example `https://example.net/credentials.cgi` the export would look like:

```
export io.aiven.commons.envcheck.uri=https://example.com/credentials.cgi,https://example.net/credentials.cgi 
# start the kafka processes
```


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

### Workload Identity Federation (WIF_JSON)

Setting `keySource` to `WIF_JSON` lets the connector authenticate to GCP via
[Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
instead of a static service-account key. It is intended for connectors running on **AWS ECS
Fargate**: the AWS task role is exchanged for a short-lived GCP token, so there is no long-lived
key to store or rotate. The `keyfile` then holds the raw JSON of an `external_account` credential
configuration rather than a service-account key.

Only AWS external accounts are supported today. The connector reads the AWS task-role credentials
from the ECS/Fargate container-credentials endpoint, so it works where google-auth's built-in AWS
provider does not.

**Supported environments.** That endpoint (`169.254.170.2`) is exposed by the ECS agent to any ECS
task that has a `taskRoleArn`, so `WIF_JSON` works on **ECS Fargate and the ECS EC2 launch type
alike**. It does **not** support:

- **Plain EC2** — a Connect worker running on an instance profile, outside ECS.
- **An ECS task without a `taskRoleArn`**, which falls back to the host's instance profile.
- **EKS** — neither EKS Pod Identity nor IRSA is supported yet.

Note that these failures surface at **runtime, not during connector configuration validation**: the
credentials are only fetched when a token is first needed, so the connector starts successfully and
then fails its first BigQuery call with
`Environment variable AWS_CONTAINER_CREDENTIALS_RELATIVE_URI is not set`.

Connector configuration:

```json
{
  "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
  "topics": "users, clicks, payments",
  "tasks.max": "3",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",

  "project": "kafka-ingest-testing",
  "defaultDataset": "kcbq-example",
  "keySource": "WIF_JSON",
  "keyfile": "{ ... external_account JSON, as a single string ... }"
}
```

Example `external_account` keyfile. It has **no `credential_source`**: the connector supplies the
AWS credentials itself, so the block serves no purpose. A keyfile that does carry one still works —
the connector strips it before validation, reading only `regional_cred_verification_url` from it.
Replace the `<...>` placeholders:

```json
{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<POOL_ID>/providers/<PROVIDER_ID>",
  "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
  "token_url": "https://sts.googleapis.com/v1/token",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<SA_EMAIL>:generateAccessToken"
}
```

**AWS/GCP setup prerequisites** (configured outside the connector):

- The ECS task must run with an IAM **task role** (`taskRoleArn` in the task definition). That role
  is the AWS identity federated into GCP, and ECS injects `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`
  (the credentials endpoint the connector reads) automatically — there is nothing to set in the
  connector or keyfile for it.
- The Workload Identity Pool **AWS provider** attribute mapping must keep `google.subject` ≤ 127
  bytes — map it to the normalized role ARN (`arn:aws:iam::<ACCOUNT>:role/<ROLE>`), not the full
  assumed-role ARN with session name.
- Grant `roles/iam.workloadIdentityUser` on the target service account, bound to
  `principalSet://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<POOL_ID>/attribute.aws_role/arn:aws:sts::<ACCOUNT>:assumed-role/<ROLE>`.
- The service account needs `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`.
- `AWS_REGION` (or `AWS_DEFAULT_REGION`) must be set — the region is part of the STS request
  signature, and there is no default. Fargate always injects it. The ECS **EC2 launch type** only
  injects it with container agent v1.104.0 or later (ECS-optimized AMI `20260615`+); on older agents
  it is absent and the connector will fail on its first BigQuery call. Set it explicitly in the task
  definition to be safe on both.

Verifying a deployment, including how to exercise the credential-fetch retry path, is covered
under [Integration test setup](#integration-test-setup).

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
mvn install -DskipITs
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

#### Manual verification on ECS Fargate (`keySource=WIF_JSON`)

The AWS→GCP path has no automated coverage: it only works inside an ECS task against a configured
GCP Workload Identity Pool. Verify a deployment by hand:

1. Deploy with `keySource=WIF_JSON` and the `external_account` keyfile, produce records, and confirm
   rows land in BigQuery and consumer offsets advance.
2. **Let it run past ~6 hours** to cover AWS credential rotation: the sink keeps writing, with no
   `Unable to refresh sourceCredentials`. With `DEBUG` logging, each refresh logs
   `Obtained temporary AWS credentials from ECS/Fargate container endpoint`.

The credential fetch is retried on connection failures, HTTP 5xx and HTTP 429 — 3 retries,
exponential backoff with full jitter, capped at 8s. Any other non-200, a malformed response, or a
missing `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` fails immediately. The real endpoint cannot be made
to fail on demand, so to exercise that path:

- **Retry and recovery — locally, no ECS needed.** The endpoint address is only special by
  convention, so bind it to loopback (`sudo ip addr add 169.254.170.2/32 dev lo`, or
  `sudo ifconfig lo0 alias 169.254.170.2` on macOS) and serve a stub on port 80 that answers `503`,
  `503`, then a normal credentials payload. Set `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` and
  `AWS_REGION`, start Connect, and expect two `retrying in <ms> ms` warnings followed by a
  successful fetch. This drives the unmodified production path. Remove the alias afterwards.
- **Fail-fast — on a real ECS task.** Point `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` at a path that
  does not exist; the 4xx must fail on the first attempt, with no `retrying in` lines.

Retry cannot be forced on Fargate itself: there is no host access or `NET_ADMIN`, so the iptables
blackhole of `169.254.170.2` that works on the ECS EC2 launch type is unavailable.
