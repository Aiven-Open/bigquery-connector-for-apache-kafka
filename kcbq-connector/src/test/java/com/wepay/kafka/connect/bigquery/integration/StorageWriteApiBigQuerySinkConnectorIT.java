package com.wepay.kafka.connect.bigquery.integration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import io.confluent.connect.avro.AvroConverter;
import io.debezium.time.Date;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public class StorageWriteApiBigQuerySinkConnectorIT extends BaseConnectorIT {

  protected static final long COMMIT_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(2);
  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiBigQuerySinkConnectorIT.class);
  private static final String CONNECTOR_NAME = "bigquery-storage-api-sink-connector";
  private static final String KAFKA_FIELD_NAME = "kafkaKey";
  private static final int TASKS_MAX = 3;
  private static final long NUM_RECORDS_PRODUCED = 5 * TASKS_MAX;
  private static SchemaRegistryTestUtils schemaRegistry;
  private static String schemaRegistryUrl;
  private Schema valueSchema;
  private BigQuery bigQuery;
  private Schema keySchema;
  private Converter keyConverter;
  private Converter valueConverter;

  @Before
  public void setup() throws Exception {
    startConnect();
    bigQuery = newBigQuery();
    schemaRegistry = new SchemaRegistryTestUtils(connect.kafka().bootstrapServers());
    schemaRegistry.start();
    schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

    Schema subStructSchema = SchemaBuilder.struct()
        .field("ssf1", Schema.INT64_SCHEMA)
        .field("ssf2", Schema.BOOLEAN_SCHEMA)
        .build();

    Schema nestedStructSchema = SchemaBuilder.struct()
        .field("sf1", Schema.STRING_SCHEMA)
        .field("sf2", subStructSchema)
        .field("sf3", Schema.FLOAT64_SCHEMA)
        .build();

    Schema primitivesSchema = SchemaBuilder.struct()
        .field("boolean_field", Schema.BOOLEAN_SCHEMA)
        .field("float32_field", Schema.FLOAT32_SCHEMA)
        .field("float64_field", Schema.FLOAT64_SCHEMA)
        .field("int8_field", Schema.INT8_SCHEMA)
        .field("int16_field", Schema.INT16_SCHEMA)
        .field("int32_field", Schema.INT32_SCHEMA)
        .field("int64_field", Schema.INT64_SCHEMA)
        .field("string_field", Schema.STRING_SCHEMA);

    Schema logicalsSchema = SchemaBuilder.struct()
        // dlf = "Debezium logical field"
        .field("dlf1", Timestamp.builder().optional().build())
        .field("dlf2", Time.builder().optional().build())
        .field("dlf3", Date.builder().optional().build())
        // klf = "Kafka logical field"
        .field("klf1", org.apache.kafka.connect.data.Timestamp.builder().optional().build())
        .field("klf2", org.apache.kafka.connect.data.Time.builder().optional().build())
        .field("klf3", org.apache.kafka.connect.data.Date.builder().optional().build())
        .field("klf4", org.apache.kafka.connect.data.Decimal.builder(5).optional().build())
        .build();

    Schema arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA);

    valueSchema = SchemaBuilder.struct()
        .optional()
        .field("f1", Schema.STRING_SCHEMA)
        .field("f2", Schema.BOOLEAN_SCHEMA)
        .field("f3", Schema.FLOAT64_SCHEMA)
        .field("bytes_field", Schema.OPTIONAL_BYTES_SCHEMA)
        .field("nested_field", nestedStructSchema)
        .field("primitives_field", primitivesSchema)
        .field("logicals_field", logicalsSchema)
        .field("array_field", arraySchema)
        .build();

    keySchema = SchemaBuilder.struct()
        .field("k1", Schema.INT64_SCHEMA)
        .build();
  }

  @After
  public void close() throws Exception {
    bigQuery = null;

    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }
    stopConnect();
  }

  @Test
  public void testBaseJson() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-append-json");
    final String table = sanitizedTable(topic);

    // create topic
    connect.kafka().createTopic(topic, TASKS_MAX);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // create the table with the correct schema
    createTable(table, false);

    // setup props for the sink connector
    Map<String, String> props = configs(topic);
    // use the JSON converter with schemas enabled
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    props.remove(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    initialiseJsonConverters();

    //produce records
    produceJsonRecords(topic);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // verify records are present.
    List<List<Object>> testRows;
    try {
      testRows = readAllRows(bigQuery, table, "f3");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
  }

  @Test
  public void testBaseAvro() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-append");
    final String table = sanitizedTable(topic);

    // create topic
    connect.kafka().createTopic(topic, TASKS_MAX);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // setup props for the sink connector
    Map<String, String> props = configs(topic);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    initialiseAvroConverters();

    //produce records
    produceAvroRecords(topic);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(
        CONNECTOR_NAME, Collections.singleton(topic), NUM_RECORDS_PRODUCED, TASKS_MAX, COMMIT_MAX_DURATION_MS);

    // verify records are present.
    List<List<Object>> testRows;
    try {
      testRows = readAllRows(bigQuery, table, "f3");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
  }

  @Test
  public void testAvroWithSchemaUpdate() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-schema-update-append");
    final String table = sanitizedTable(topic);

    // create topic
    connect.kafka().createTopic(topic, TASKS_MAX);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // create the table with an incomplete schema
    createTable(table, true);

    // setup props + schema update props for the sink connector
    Map<String, String> props = configs(topic);

    props.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "true");
    props.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");
    props.put(BigQuerySinkConfig.ALLOW_SCHEMA_UNIONIZATION_CONFIG, "true");
    props.put(BigQuerySinkConfig.ALL_BQ_FIELDS_NULLABLE_CONFIG, "true");

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    initialiseAvroConverters();

    //produce records
    produceAvroRecords(topic);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // verify records are present.
    List<List<Object>> testRows;
    try {
      testRows = readAllRows(bigQuery, table, "f3");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
  }

  @Test
  public void testTopicsRegex() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-append-json-topics-regex");
    final String table = sanitizedTable(topic);

    // create topic
    connect.kafka().createTopic(topic, TASKS_MAX);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // create the table with the correct schema
    createTable(table, false);

    // setup props for the sink connector
    Map<String, String> props = configs(topic);

    // use topics regex instead of topics list
    props.remove(BigQuerySinkConfig.TOPICS_CONFIG);
    props.put(SinkTask.TOPICS_REGEX_CONFIG, topic);

    // use the JSON converter with schemas enabled
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    props.remove(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    initialiseJsonConverters();

    //produce records
    produceJsonRecords(topic);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // verify records are present.
    List<List<Object>> testRows;
    try {
      testRows = readAllRows(bigQuery, table, "f3");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
  }

  @Test
  public void testFailWhenTableDoesNotExistAndCreationDisabled() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-append-fail");
    final String table = sanitizedTable(topic);

    // create topic
    connect.kafka().createTopic(topic, TASKS_MAX);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // setup props for the sink connector
    Map<String, String> props = configs(topic);
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    initialiseAvroConverters();

    //produce records
    produceAvroRecords(topic);

    connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
        CONNECTOR_NAME,
        TASKS_MAX,
        "Tasks should have failed when writing to nonexistent table "
            + "with automatic table creation disabled"
    );
  }

  @Test
  public void testAvroLargeBatches() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-append-large-batches");
    final String table = sanitizedTable(topic);

    // pre-create the table
    createPerformanceTestingTable(table);

    int tasksMax = 1;
    long numRecords = 100_000;

    // create topic
    connect.kafka().createTopic(topic, tasksMax);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // Instantiate the converters we'll use to send records to the connector
    initialiseAvroConverters();

    //produce records, each with a 100 byte value
    produceAvroRecords(topic, numRecords, 100);

    // setup props for the sink connector
    Map<String, String> props = configs(topic);
    props.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));

    // read as many records from Kafka in a single poll as possible
    props.put(
        CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + MAX_POLL_RECORDS_CONFIG,
        Long.toString(numRecords)
    );
    props.put(
        CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + MAX_PARTITION_FETCH_BYTES_CONFIG,
        Integer.toString(Integer.MAX_VALUE)
    );
    props.put(
        CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + FETCH_MAX_BYTES_CONFIG,
        Integer.toString(Integer.MAX_VALUE)
    );

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, tasksMax);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(
        CONNECTOR_NAME, Collections.singleton(topic), numRecords, tasksMax, TimeUnit.MINUTES.toMillis(5));

    connect.deleteConnector(CONNECTOR_NAME);

    final AtomicLong numRows = new AtomicLong();
    TestUtils.waitForCondition(
        () -> {
          numRows.set(countRows(bigQuery, table));
          assertEquals(numRecords, numRows.get());
          return true;
        },
        10_000L,
        () -> "Table should contain " + numRecords
            + " rows, but has " + numRows.get() + " instead"
    );
  }

  @Test
  @Ignore("TODO: Handle 'java.lang.RuntimeException: Request has waited in inflight queue for <duration> for writer <writer>, which is over maximum wait time PT5M'")
  public void testAvroHighThroughput() throws InterruptedException {
    // create topic in Kafka
    final String topic = suffixedTableOrTopic("storage-api-append-high-throughput");
    final String table = sanitizedTable(topic);

    // pre-create the table
    createPerformanceTestingTable(table);

    int tasksMax = 10;
    long numRecords = 10_000_000;

    // create topic
    connect.kafka().createTopic(topic, tasksMax);

    // clean table
    TableClearer.clearTables(bigQuery, dataset(), table);

    // Instantiate the converters we'll use to send records to the connector
    initialiseAvroConverters();

    //produce records, each with a 100 byte value
    produceAvroRecords(topic, numRecords, 100);

    // setup props for the sink connector
    Map<String, String> props = configs(topic);
    props.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));

    // the Storage Write API allows for 10MB per write; try to get close to that
    // for each poll
    props.put(
        CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + FETCH_MAX_BYTES_CONFIG,
        Integer.toString(9 * 1024 * 1024)
    );
    props.put(
        CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + MAX_PARTITION_FETCH_BYTES_CONFIG,
        Integer.toString(Integer.MAX_VALUE)
    );
    props.put(
        CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + MAX_POLL_RECORDS_CONFIG,
        Long.toString(numRecords)
    );

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, tasksMax);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(
        CONNECTOR_NAME,
        Collections.singleton(topic),
        numRecords,
        tasksMax,
        TimeUnit.MINUTES.toMillis(10)
    );

    connect.deleteConnector(CONNECTOR_NAME);

    final AtomicLong numRows = new AtomicLong();
    TestUtils.waitForCondition(
        () -> {
          numRows.set(countRows(bigQuery, table));
          assertEquals(numRecords, numRows.get());
          return true;
        },
        10_000L,
        () -> "Table should contain " + numRecords
            + " rows, but has " + numRows.get() + " instead"
    );
  }

  private void createPerformanceTestingTable(String table) {
    // pre-create the table
    com.google.cloud.bigquery.Schema tableSchema = com.google.cloud.bigquery.Schema.of(
        Field.of("f1", StandardSQLTypeName.STRING),
        Field.of(KAFKA_FIELD_NAME, StandardSQLTypeName.STRUCT, Field.of("k1", StandardSQLTypeName.STRING))
    );
    try {
      BigQueryTestUtils.createPartitionedTable(bigQuery, dataset(), table, tableSchema);
    } catch (BigQueryException ex) {
      if (!ex.getError().getReason().equalsIgnoreCase("duplicate")) {
        throw new ConnectException("Failed to create table: ", ex);
      } else {
        logger.info("Table {} already exist", table);
      }
    }
  }

  private void createTable(String table, boolean incompleteSchema) {
    com.google.cloud.bigquery.Schema tableSchema;
    if (incompleteSchema) {
      tableSchema = com.google.cloud.bigquery.Schema.of(
          Field.of("f1", StandardSQLTypeName.STRING),
          Field.of("f2", StandardSQLTypeName.BOOL)
      );
    } else {
      FieldList subStructFields = FieldList.of(
          Field.of("ssf1", StandardSQLTypeName.INT64),
          Field.of("ssf2", StandardSQLTypeName.BOOL)
      );

      FieldList nestedStructFields = FieldList.of(
          Field.of("sf1", StandardSQLTypeName.STRING),
          Field.newBuilder("sf2", StandardSQLTypeName.STRUCT, subStructFields).build(),
          Field.of("sf3", StandardSQLTypeName.FLOAT64)
      );

      FieldList primitivesFields = FieldList.of(
          Field.of("boolean_field", StandardSQLTypeName.BOOL),
          Field.of("float32_field", StandardSQLTypeName.FLOAT64),
          Field.of("float64_field", StandardSQLTypeName.FLOAT64),
          Field.of("int8_field", StandardSQLTypeName.INT64),
          Field.of("int16_field", StandardSQLTypeName.INT64),
          Field.of("int32_field", StandardSQLTypeName.INT64),
          Field.of("int64_field", StandardSQLTypeName.INT64),
          Field.of("string_field", StandardSQLTypeName.STRING)
      );

      FieldList logicalsField = FieldList.of(
          Field.of("dlf1", StandardSQLTypeName.TIMESTAMP),
          Field.of("dlf2", StandardSQLTypeName.TIME),
          Field.of("dlf3", StandardSQLTypeName.DATE),
          Field.of("klf1", StandardSQLTypeName.TIMESTAMP),
          Field.of("klf2", StandardSQLTypeName.TIME),
          Field.of("klf3", StandardSQLTypeName.DATE),
          Field.newBuilder("klf4", StandardSQLTypeName.BIGNUMERIC)
              .setScale(5L)
              .setPrecision(15L)
              .build()
      );

      tableSchema = com.google.cloud.bigquery.Schema.of(
          Field.of("f1", StandardSQLTypeName.STRING),
          Field.of("f2", StandardSQLTypeName.BOOL),
          Field.of("f3", StandardSQLTypeName.FLOAT64),
          Field.of("bytes_field", StandardSQLTypeName.BYTES),
          Field.of("nested_field", StandardSQLTypeName.STRUCT, nestedStructFields),
          Field.of("primitives_field", StandardSQLTypeName.STRUCT, primitivesFields),
          Field.of("logicals_field", StandardSQLTypeName.STRUCT, logicalsField),
          Field.newBuilder("array_field", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build()
      );
    }

    try {
      BigQueryTestUtils.createPartitionedTable(bigQuery, dataset(), table, tableSchema);
    } catch (BigQueryException ex) {
      if (!ex.getError().getReason().equalsIgnoreCase("duplicate"))
        throw new ConnectException("Failed to create table: ", ex);
      else
        logger.info("Table {} already exist", table);
    }
  }

  private void produceAvroRecords(String topic) {
    produceAvroRecords(topic, NUM_RECORDS_PRODUCED);
  }

  private void produceAvroRecords(String topic, long numRecords) {
    produceAvroRecords(
        topic,
        numRecords,
        keySchema,
        valueSchema,
        this::avroKey,
        this::avroValue
    );
  }

  private void produceAvroRecords(String topic, long numRecords, int valueSize) {
    // AAAAAAAAAAAAAAAAAAAAAAAAAAA
    String largeField = String.join("", Collections.nCopies(valueSize, "A"));

    Schema largeValueSchema = SchemaBuilder.struct()
        .field("f1", Schema.STRING_SCHEMA)
        .build();

    produceAvroRecords(
        topic,
        numRecords,
        keySchema,
        largeValueSchema,
        this::avroKey,
        i -> new Struct(largeValueSchema).put("f1", largeField)
    );
  }

  private void produceAvroRecords(
      String topic,
      long numRecords,
      Schema keySchema,
      Schema valueSchema,
      LongFunction<Object> computeKey,
      LongFunction<Object> computeValue
  ) {
    List<List<SchemaAndValue>> records = new ArrayList<>();

    // Prepare records
    for (long i = 0; i < numRecords; i++) {
      List<SchemaAndValue> record = new ArrayList<>();

      SchemaAndValue keySchemaAndValue = new SchemaAndValue(
          keySchema,
          computeKey.apply(i)
      );
      SchemaAndValue schemaAndValue = new SchemaAndValue(
          valueSchema,
          computeValue.apply(i)
      );

      record.add(keySchemaAndValue);
      record.add(schemaAndValue);

      records.add(record);
    }

    // send prepared records
    schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);

  }

  private void initialiseAvroConverters() {
    keyConverter = new AvroConverter();
    valueConverter = new AvroConverter();
    keyConverter.configure(Collections.singletonMap(
            SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        ), true
    );
    valueConverter.configure(Collections.singletonMap(
            SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        ), false
    );
  }

  private void produceJsonRecords(String topic) {
    // Prepare records
    for (long iteration = 0; iteration < NUM_RECORDS_PRODUCED; iteration++) {
      Map<String, Object> primitivesValue = new HashMap<>();
      primitivesValue.put("boolean_field", iteration % 3 == 1);
      primitivesValue.put("float32_field", iteration * 1.5f);
      primitivesValue.put("float64_field", iteration * 0.5);
      primitivesValue.put("int8_field", (byte) (iteration % 10));
      primitivesValue.put("int16_field", (short) (iteration % 30 + 1));
      primitivesValue.put("int32_field", (int) (-1 * (iteration % 100)));
      primitivesValue.put("int64_field", iteration * 10);
      primitivesValue.put("string_field", Long.toString(iteration * 123));

      Map<String, Object> logicalsValue = new HashMap<>();
      long timestampMicros = 1707835187396371L;
      long microsPerDay = 86400000000L;
      // BigQuery doesn't handle integer values for time columns very well; omit
      // these fields (i.e., dlf2 and klf2) from JSON testing
      logicalsValue.put("dlf1", timestampMicros); // timestamp
      logicalsValue.put("dlf2", 999999); // time
      logicalsValue.put("dlf3", timestampMicros / microsPerDay); // date
      logicalsValue.put("klf1", timestampMicros); // timestamp
      logicalsValue.put("klf3", timestampMicros / microsPerDay); // date
      logicalsValue.put("klf4", 5432); // decimal

      Map<String, Object> subValue = new HashMap<>();
      subValue.put("ssf1", iteration / 2);
      subValue.put("ssf2", false);

      Map<String, Object> nestedValue = new HashMap<>();
      nestedValue.put("sf1", "sv1");
      nestedValue.put("sf2", subValue);
      nestedValue.put("sf3", iteration * 1.0);

      List<String> arrayValue = LongStream.of(iteration % 10)
          .mapToObj(l -> "array element " + l)
          .collect(Collectors.toList());

      // no bytes value since that gets serialized as a base 64 string by the JSON
      // converter, which we don't convert to a byte array before sending to bigquery,
      // causing insertions to fail

      Map<String, Object> kafkaValue = new HashMap<>();
      kafkaValue.put("f1", "api" + iteration);
      kafkaValue.put("f2", iteration % 2 == 0);
      kafkaValue.put("f3", iteration * 0.01);
      kafkaValue.put("nested_field", nestedValue);
      kafkaValue.put("primitives_field", primitivesValue);
      kafkaValue.put("logicals_field", logicalsValue);
      kafkaValue.put("array_field", arrayValue);

      connect.kafka().produce(
          topic,
          null,
          new String(valueConverter.fromConnectData(topic, null, kafkaValue)));
    }
  }

  private void initialiseJsonConverters() {
    keyConverter = converter(true);
    valueConverter = converter(false);
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  protected Map<String, String> configs(String topic) {
    Map<String, String> result = baseConnectorProps(1);
    result.put(ConnectorConfig.TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    result.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    result.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    result.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    result.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");
    // use the Avro converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    result.put(
        ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl);
    result.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    result.put(
        ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl);

    result.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, KAFKA_FIELD_NAME);

    result.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

    return result;
  }

  private Struct avroValue(long iteration) {
    Struct primitivesStruct = new Struct(valueSchema.field("primitives_field").schema());
    primitivesStruct.put("boolean_field", iteration % 3 == 1);
    primitivesStruct.put("float32_field", iteration * 1.5f);
    primitivesStruct.put("float64_field", iteration * 0.5);
    primitivesStruct.put("int8_field", (byte) (iteration % 10));
    primitivesStruct.put("int16_field", (short) (iteration % 30 + 1));
    primitivesStruct.put("int32_field", (int) (-1 * (iteration % 100)));
    primitivesStruct.put("int64_field", iteration * 10);
    primitivesStruct.put("string_field", Long.toString(iteration * 123));

    Struct logicalsStruct = new Struct(valueSchema.field("logicals_field").schema());
    long timestampMs = 1707835187396L;
    int msPerDay = 86400000;
    int time = (int) (timestampMs % msPerDay);
    int date = (int) (timestampMs / msPerDay);
    Schema klf1Schema = logicalsStruct.schema().field("klf1").schema();
    java.util.Date klf1Value = org.apache.kafka.connect.data.Timestamp.toLogical(klf1Schema, timestampMs);
    Schema klf2Schema = logicalsStruct.schema().field("klf2").schema();
    java.util.Date klf2Value = org.apache.kafka.connect.data.Time.toLogical(klf2Schema, time);
    Schema klf3Schema = logicalsStruct.schema().field("klf3").schema();
    java.util.Date klf3Value = org.apache.kafka.connect.data.Date.toLogical(klf3Schema, date);
    logicalsStruct
        .put("dlf1", timestampMs)
        .put("dlf2", time)
        .put("dlf3", date)
        .put("klf1", klf1Value)
        .put("klf2", klf2Value)
        .put("klf3", klf3Value)
        .put("klf4", BigDecimal.valueOf(6543).setScale(5));

    Struct subStruct = new Struct(valueSchema
        .field("nested_field").schema()
        .field("sf2").schema()
    );
    subStruct.put("ssf1", iteration / 2);
    subStruct.put("ssf2", false);

    Struct nestedStruct = new Struct(valueSchema.field("nested_field").schema());
    nestedStruct.put("sf1", "sv1");
    nestedStruct.put("sf2", subStruct);
    nestedStruct.put("sf3", iteration * 1.0);

    List<String> arrayValue = LongStream.of(iteration % 10)
        .mapToObj(l -> "array element " + l)
        .collect(Collectors.toList());

    byte[] bytesValue = new byte[(int) iteration % 4];
    for (int i = 0; i < bytesValue.length; i++)
      bytesValue[i] = (byte) i;

    return new Struct(valueSchema)
        .put("f1", "api" + iteration)
        .put("f2", iteration % 2 == 0)
        .put("f3", iteration * 0.01)
        .put("bytes_field", bytesValue)
        .put("nested_field", nestedStruct)
        .put("primitives_field", primitivesStruct)
        .put("logicals_field", logicalsStruct)
        .put("array_field", arrayValue);
  }

  private Struct avroKey(long iteration) {
    return new Struct(keySchema).put("k1", iteration);
  }

  private Set<Object> expectedRows() {
    Set<Object> rows = new HashSet<>();
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      rows.add("api" + i);
    }
    return rows;
  }

  protected String topic(String baseName) {
    return suffixedTableOrTopic(baseName);
  }

}
