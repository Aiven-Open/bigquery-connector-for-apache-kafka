# Add "Write Strategy" as an interface

## Reason

Currently the code base for the BigQuery connector contains a lot of spaghetti code with a significant
number of "if checks" for various paths.  There are also a number of configuration options that only 
apply to the specific paths.  For example, we have a path that writes the data to GCS before copying 
it to BigQuery in a batch.  We have a direct write strategy that uses the WriteAPI.  We have variation
that support updating data and/or deleting data.  This leads to a heavy mental load when trying to 
perform debugging, bug fixing, or adding additional features.

## Synopsis

A WriteStrategy abstract class will be created that intercepts the requests to write SinkRecords.  The 
`BigQuerySinkTask` will create one (1) instance of the WriteStrategy and use it to route data to BigQuery.

Once the strategy accepts the record it is responsible for ensuring that the data are delivered or reported
as failed. The strategy will contain a reference to an `ErrantRecordReporter`  Any record that can not be delivered
by the write strategy will be written to the ErrantRecordReporter.  Implementations of the ErrantRecordReporter
will determine if and how the errant record will be reported.

The WriteStrategy will be thread safe.  In this way we can use multiple instances of the strategy to
write different destination tables based on configuration options.  The grouping strategy is outside the
scope of the write strategy.

Each write strategy will write to one and only one BigQuery table.  Multiple write strategies may write 
to the same table.  So the cardinality is many write strategies to one BigQuery table.

## Implementation notes 

The write strategy will have a `putInterceptor` method to accept the `SinkTask.put` data.
This method will accept one `SinkRecord` at a time.

The write strategy will have a `preCommitInterceptor` method to intercept the `SinkTask.preCommit` method.
This method will accept the same argument as and return the correct value to the `SinkTask.preCommit`. 
The write strategy should return the highest offset for the topic,partition that it can commit.  If it can
not commit the topic,partition it should not return a result for that topic,partition.

The write strategy will have a `flushInterceptor` method  to intercept the `SinkTask.flush` method to complete the
writing to BigQuery.

Each write strategy will implement a 'getDescription' method  that will provide a description of how the 
write strategy works.  This information will be used in documentation.

Each write strategy implementation will include a static `getConfigDef` method that will return `ConfigDef` 
that defines the configuration options that it needs.  The configuration options do not need to be unique across
multiple implementations.  In the final configuration the variables will be prefixed with "writestrategy.config.", 
this will be handled by the BigQuerySinkTask or BigQuerySinkConnector implementation.

Each write strategy will accept `Map<String, String>` to configure the strategy.  The keys for the map will 
be the keys defined in the ConfigDef returned by the `getConfigDef` call.

