 
# Change log
 
All releases can be found at https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/releases
 
## v2.15.0
### What is changed
 
 - update util dependency and fixed race condition (#233)
 - fix Issue-111: add spotless google java formatter (#232)
 - switch to kafka-config use (#231)
 - clean up workflows (#227)
 - Merge pull request #213 from alvm/fix/retry-job-backend-error
 - Merge pull request #215 from alvm/fix/retry-dataset-not-found
 - Merge pull request #218 from Aiven-Open/Issue-217-Mitigate-KafkaDataBuilder
 - Merge pull request #224 from Aiven-Open/dependabot/maven/com.fasterxml.jackson.core-jackson-databind-2.22.1
 - fix for code scanning alert no. 1: Workflow does not contain permissions (#222)
 - fix for code scanning alert no. 2: Use of a cryptographic algorithm with insufficient key size (#223)
 - Bump com.fasterxml.jackson.core:jackson-databind from 2.21.1 to 2.22.1
 - Fix sink integration test (#220)
 - fixed checkstyle issues
 - Updated documentation. split isUpserDeleteEnabled into separate methods.
 - Deprecated KafkaDataBuilder and removed static vars.
 - Retry transient 404 "Not found: Dataset" during streaming inserts
 - Retry transient 400 jobBackendError in MergeQueries
 - Bump version to 2.15.0-SNAPSHOT
 
 
### Co-authored by
 
 - Claude Warren
 - Emmanuel Evbuomwan
 - Oleksii Molchanov
 - Tony Bui
 
 
### Full Changelog
https://github.com/Aiven-Open/${repositoryName}/compare/v2.14.0...v2.15.0
 

## v2.14.0
### What is changed

- Update putAttemptId for uniqueness across internal Bigquery write retries and GCS Batch Idempotent Load Jobs (#206)

### Co-authored by

- Claude Warren
- Veli Can Ünal

### Full Changelog

https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.13.0...v2.14.0
## v2.13.0
