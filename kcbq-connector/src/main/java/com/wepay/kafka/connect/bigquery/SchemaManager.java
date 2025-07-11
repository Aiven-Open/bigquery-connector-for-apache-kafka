/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery;


import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {

  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;
  private final boolean allowNewBqFields;
  private final boolean allowBqRequiredFieldRelaxation;
  private final boolean allowSchemaUnionization;
  private final boolean sanitizeFieldNames;
  private final Optional<String> kafkaKeyFieldName;
  private final Optional<String> kafkaDataFieldName;
  private final Optional<String> timestampPartitionFieldName;
  private final Optional<Long> partitionExpiration;
  private final Optional<List<String>> clusteringFieldName;
  private final Optional<TimePartitioning.Type> timePartitioningType;
  private final boolean intermediateTables;
  // TODO: This is a bit sloppy; we may way to allow other fields to be used as primary key
  //       at some point. Good enough for the upsert/delete + Storage Write API use case, though
  private final boolean kafkaKeyAsPrimaryKey;
  private final ConcurrentMap<TableId, Object> tableCreateLocks;
  private final ConcurrentMap<TableId, Object> tableUpdateLocks;
  private final ConcurrentMap<TableId, SchemaAndPrimaryKeyColumns> schemaCache;

  /**
   * @param schemaRetriever                Used to determine the Kafka Connect Schema that should be used for a
   *                                       given table.
   * @param schemaConverter                Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery                       Used to communicate create/update requests to BigQuery.
   * @param allowNewBqFields               If set to true, allows new fields to be added to BigQuery Schema.
   * @param allowBqRequiredFieldRelaxation If set to true, allows changing field mode from REQUIRED to NULLABLE
   * @param allowSchemaUnionization        If set to true, allows existing and new schemas to be unionized
   * @param sanitizeFieldNames             If true, sanitizes field names to adhere to BigQuery column name restrictions
   * @param kafkaKeyFieldName              The name of kafka key field to be used in BigQuery.
   *                                       If set to null, Kafka Key Field will not be included in BigQuery.
   * @param kafkaDataFieldName             The name of kafka data field to be used in BigQuery.
   *                                       If set to null, Kafka Data Field will not be included in BigQuery.
   * @param timestampPartitionFieldName    The name of the field to use for column-based time
   *                                       partitioning in BigQuery.
   *                                       If set to null, ingestion time-based partitioning will be
   *                                       used instead.
   * @param clusteringFieldName
   * @param timePartitioningType           The time partitioning type (HOUR, DAY, etc.) to use for created tables.
   * @param kafkaKeyAsPrimaryKey           Whether to use record key fields as the primary key for BigQuery tables
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery,
      boolean allowNewBqFields,
      boolean allowBqRequiredFieldRelaxation,
      boolean allowSchemaUnionization,
      boolean sanitizeFieldNames,
      Optional<String> kafkaKeyFieldName,
      Optional<String> kafkaDataFieldName,
      Optional<String> timestampPartitionFieldName,
      Optional<Long> partitionExpiration,
      Optional<List<String>> clusteringFieldName,
      Optional<TimePartitioning.Type> timePartitioningType,
      boolean kafkaKeyAsPrimaryKey
  ) {
    this(
        schemaRetriever,
        schemaConverter,
        bigQuery,
        allowNewBqFields,
        allowBqRequiredFieldRelaxation,
        allowSchemaUnionization,
        sanitizeFieldNames,
        kafkaKeyFieldName,
        kafkaDataFieldName,
        timestampPartitionFieldName,
        partitionExpiration,
        clusteringFieldName,
        timePartitioningType,
        kafkaKeyAsPrimaryKey,
        false,
        new ConcurrentHashMap<>(),
        new ConcurrentHashMap<>(),
        new ConcurrentHashMap<>());
  }

  private SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery,
      boolean allowNewBqFields,
      boolean allowBqRequiredFieldRelaxation,
      boolean allowSchemaUnionization,
      boolean sanitizeFieldNames,
      Optional<String> kafkaKeyFieldName,
      Optional<String> kafkaDataFieldName,
      Optional<String> timestampPartitionFieldName,
      Optional<Long> partitionExpiration,
      Optional<List<String>> clusteringFieldName,
      Optional<TimePartitioning.Type> timePartitioningType,
      boolean kafkaKeyAsPrimaryKey,
      boolean intermediateTables,
      ConcurrentMap<TableId, Object> tableCreateLocks,
      ConcurrentMap<TableId, Object> tableUpdateLocks,
      ConcurrentMap<TableId, SchemaAndPrimaryKeyColumns> schemaCache) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
    this.allowNewBqFields = allowNewBqFields;
    this.allowBqRequiredFieldRelaxation = allowBqRequiredFieldRelaxation;
    this.allowSchemaUnionization = allowSchemaUnionization;
    this.sanitizeFieldNames = sanitizeFieldNames;
    this.kafkaKeyFieldName = kafkaKeyFieldName;
    this.kafkaDataFieldName = kafkaDataFieldName;
    this.timestampPartitionFieldName = timestampPartitionFieldName;
    this.partitionExpiration = partitionExpiration;
    this.clusteringFieldName = clusteringFieldName;
    this.timePartitioningType = timePartitioningType;
    this.kafkaKeyAsPrimaryKey = kafkaKeyAsPrimaryKey;
    this.intermediateTables = intermediateTables;
    this.tableCreateLocks = tableCreateLocks;
    this.tableUpdateLocks = tableUpdateLocks;
    this.schemaCache = schemaCache;

    if (kafkaKeyAsPrimaryKey && !kafkaKeyFieldName.isPresent()) {
      throw new IllegalArgumentException(
          "Cannot use Kafka record keys as primary keys without also specifying a Kafka key field name"
      );
    }
  }

  public SchemaManager forIntermediateTables() {
    return new SchemaManager(
        schemaRetriever,
        schemaConverter,
        bigQuery,
        allowNewBqFields,
        allowBqRequiredFieldRelaxation,
        allowSchemaUnionization,
        sanitizeFieldNames,
        kafkaKeyFieldName,
        kafkaDataFieldName,
        timestampPartitionFieldName,
        partitionExpiration,
        clusteringFieldName,
        timePartitioningType,
        kafkaKeyAsPrimaryKey,
        true,
        tableCreateLocks,
        tableUpdateLocks,
        schemaCache
    );
  }

  /**
   * Fetch the most recent schema for the given table, assuming it has been created and/or updated
   * over the lifetime of this schema manager.
   *
   * @param table the table to fetch the schema for; may be null
   * @return the latest schema for that table; may be null if the table does not exist or has not
   * been created or updated by this schema manager
   */
  public com.google.cloud.bigquery.Schema cachedSchema(TableId table) {
    return schemaCache.get(table).getSchema();
  }

  /**
   * Create a new table in BigQuery, if it doesn't already exist. Otherwise, update the existing
   * table to use the most-current schema.
   *
   * @param table   The BigQuery table to create,
   * @param records The sink records used to determine the schema.
   */
  public void createOrUpdateTable(TableId table, List<SinkRecord> records) {
    synchronized (lock(tableCreateLocks, table)) {
      if (bigQuery.getTable(table) == null) {
        logger.debug("{} doesn't exist; creating instead of updating", table(table));
        if (createTable(table, records)) {
          return;
        }
      }
    }

    // Table already existed; attempt to update instead
    logger.debug("{} already exists; updating instead of creating", table(table));
    updateSchema(table, records);
  }

  /**
   * Create a new table in BigQuery.
   *
   * @param table   The BigQuery table to create.
   * @param records The sink records used to determine the schema.
   * @return whether the table had to be created; if the table already existed, will return false
   */
  public boolean createTable(TableId table, List<SinkRecord> records) {
    synchronized (lock(tableCreateLocks, table)) {
      if (schemaCache.containsKey(table)) {
        // Table already exists; noop
        logger.debug("Skipping create of {} as it should already exist or appear very soon", table(table));
        return false;
      }
      TableInfo tableInfo = getTableInfo(table, records, true);
      logger.info("Attempting to create {} with schema {}",
          table(table), tableInfo.getDefinition().getSchema());
      try {
        bigQuery.create(tableInfo);
        logger.debug("Successfully created {}", table(table));
        schemaCache.put(table, SchemaAndPrimaryKeyColumns.of(tableInfo));
        return true;
      } catch (BigQueryException e) {
        if (e.getCode() == 409) {
          logger.debug("Failed to create {} as it already exists (possibly created by another task)", table(table));
          schemaCache.put(table, readTableSchema(table));
          return false;
        }
        throw e;
      }
    }
  }

  /**
   * Update an existing table in BigQuery.
   *
   * @param table   The BigQuery table to update.
   * @param records The sink records used to update the schema.
   */
  public void updateSchema(TableId table, List<SinkRecord> records) {
    synchronized (lock(tableUpdateLocks, table)) {
      TableInfo tableInfo = getTableInfo(table, records, false);
      if (!schemaCache.containsKey(table)) {
        schemaCache.put(table, readTableSchema(table));
      }

      SchemaAndPrimaryKeyColumns schemaAndPrimaryKeyColumns = SchemaAndPrimaryKeyColumns.of(tableInfo);
      if (!schemaCache.get(table).equals(schemaAndPrimaryKeyColumns)) {
        logger.info("Attempting to update {} with schema {}",
            table(table), tableInfo.getDefinition().getSchema());
        bigQuery.update(tableInfo);
        logger.debug("Successfully updated {}", table(table));
        schemaCache.put(table, schemaAndPrimaryKeyColumns);
      } else {
        logger.debug("Skipping update of {} since current schema should be compatible", table(table));
      }
    }
  }

  /**
   * Returns the {@link TableInfo} instance of a bigQuery Table
   *
   * @param table        The BigQuery table to return the table info
   * @param records      The sink records used to determine the schema for constructing the table info
   * @param createSchema Flag to determine if we are creating a new table schema or updating an existing table schema
   * @return The resulting BigQuery table information
   */
  private TableInfo getTableInfo(TableId table, List<SinkRecord> records, Boolean createSchema) {
    SchemaAndPrimaryKeyColumns proposedSchema;
    String tableDescription;
    try {
      proposedSchema = getAndValidateProposedSchema(table, records);
      tableDescription = getUnionizedTableDescription(records);
    } catch (BigQueryConnectException exception) {
      throw new BigQueryConnectException("Failed to unionize schemas of records for the table " + table, exception);
    }
    return constructTableInfo(
        table,
        proposedSchema.getSchema(),
        proposedSchema.getPrimaryKeyColumns(),
        tableDescription,
        createSchema
    );
  }

  @VisibleForTesting
  SchemaAndPrimaryKeyColumns getAndValidateProposedSchema(
      TableId table, List<SinkRecord> records) {
    SchemaAndPrimaryKeyColumns result;
    if (allowSchemaUnionization) {
      List<SchemaAndPrimaryKeyColumns> bigQuerySchemas = getSchemasList(table, records);
      result = getUnionizedSchema(bigQuerySchemas);
    } else {
      SchemaAndPrimaryKeyColumns existingSchema = readTableSchema(table);
      SinkRecord recordToConvert = getRecordToConvert(records);
      if (recordToConvert == null) {
        String errorMessage = "Could not convert to BigQuery schema with a batch of tombstone records.";
        if (existingSchema == null) {
          throw new BigQueryConnectException(errorMessage);
        }
        logger.debug(errorMessage + " Will fall back to existing schema.");
        return existingSchema;
      }
      result = convertRecordSchema(recordToConvert);
      if (existingSchema != null) {
        validateSchemaChange(existingSchema.getSchema(), result.getSchema());
        if (allowBqRequiredFieldRelaxation) {
          com.google.cloud.bigquery.Schema resultSchema = relaxFieldsWhereNecessary(existingSchema.getSchema(), result.getSchema());
          result = new SchemaAndPrimaryKeyColumns(
              resultSchema,
              result.primaryKeyColumns
          );
        }
      }
    }
    return result;
  }

  /**
   * Returns a list of BigQuery schemas of the specified table and the sink records
   *
   * @param table   The BigQuery table's schema to add to the list of schemas
   * @param records The sink records' schemas to add to the list of schemas
   * @return List of BigQuery schemas
   */
  private List<SchemaAndPrimaryKeyColumns> getSchemasList(TableId table, List<SinkRecord> records) {
    List<SchemaAndPrimaryKeyColumns> bigQuerySchemas = new ArrayList<>();
    Optional.ofNullable(readTableSchema(table)).ifPresent(bigQuerySchemas::add);
    for (SinkRecord record : records) {
      Schema kafkaValueSchema = schemaRetriever.retrieveValueSchema(record);
      if (kafkaValueSchema == null) {
        continue;
      }
      bigQuerySchemas.add(convertRecordSchema(record));
    }
    return bigQuerySchemas;
  }

  /**
   * Gets a regular record from the given batch of SinkRecord for schema conversion. This is needed
   * when delete is enabled, because a tombstone record has null value, thus null value schema.
   * Converting null value schema to BigQuery schema is not possible.
   *
   * @param records List of SinkRecord to look for.
   * @return a regular record or null if the whole batch are all tombstone records.
   */
  private SinkRecord getRecordToConvert(List<SinkRecord> records) {
    for (int i = records.size() - 1; i >= 0; i--) {
      SinkRecord record = records.get(i);
      if (schemaRetriever.retrieveValueSchema(record) != null) {
        return record;
      }
    }
    return null;
  }

  private SchemaAndPrimaryKeyColumns convertRecordSchema(SinkRecord record) {
    Schema kafkaValueSchema = schemaRetriever.retrieveValueSchema(record);
    Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveKeySchema(record) : null;
    return getBigQuerySchema(kafkaKeySchema, kafkaValueSchema);
  }

  /**
   * Returns a unionized schema from a list of BigQuery schemas
   *
   * @param schemas The list of BigQuery schemas to unionize
   * @return The resulting unionized BigQuery schema
   */
  private SchemaAndPrimaryKeyColumns getUnionizedSchema(List<SchemaAndPrimaryKeyColumns> schemas) {
    SchemaAndPrimaryKeyColumns currentSchema = schemas.get(0);
    SchemaAndPrimaryKeyColumns proposedSchema;
    for (int i = 1; i < schemas.size(); i++) {
      proposedSchema = unionizeSchemas(currentSchema, schemas.get(i));
      validateSchemaChange(currentSchema.getSchema(), proposedSchema.getSchema());
      currentSchema = proposedSchema;
    }
    return currentSchema;
  }

  private Field unionizeFields(Field firstField, Field secondField) {
    if (secondField == null) {
      if (!Field.Mode.REPEATED.equals(firstField.getMode())) {
        return firstField.toBuilder().setMode(Field.Mode.NULLABLE).build();
      } else {
        return firstField;
      }
    }

    checkState(
        firstField.getName().equalsIgnoreCase(secondField.getName()),
        String.format(
            "Cannot perform union operation on two fields having different names. "
                + "Field names are '%s' and '%s'.",
            firstField.getName(),
            secondField.getName()
        )
    );
    checkState(
        firstField.getType() == secondField.getType(),
        String.format(
            "Cannot perform union operation on two fields having different datatypes. "
              + "Field name is '%s' and datatypes are '%s' and '%s'.",
            firstField.getName(),
            firstField.getType(),
            secondField.getType()
        )
    );

    Field.Builder retBuilder = firstField.toBuilder();
    if (isFieldRelaxation(firstField, secondField)) {
      retBuilder.setMode(secondField.getMode());
    }
    if (firstField.getType() == LegacySQLTypeName.RECORD) {
      Map<String, Field> firstSubFields = subFields(firstField);
      Map<String, Field> secondSubFields = subFields(secondField);
      Map<String, Field> unionizedSubFields = new LinkedHashMap<>();

      firstSubFields.forEach((name, firstSubField) -> {
        Field secondSubField = secondSubFields.get(name);
        unionizedSubFields.put(name, unionizeFields(firstSubField, secondSubField));
      });
      maybeAddToUnionizedFields(secondSubFields, unionizedSubFields);
      retBuilder.setType(LegacySQLTypeName.RECORD,
          unionizedSubFields.values().toArray(new Field[]{}));
    }
    return retBuilder.build();
  }

  /**
   * Returns a single unionized BigQuery schema from two BigQuery schemas.
   *
   * @param firstSchema  The first BigQuery schema to unionize
   * @param secondSchema The second BigQuery schema to unionize
   * @return The resulting unionized BigQuery schema
   */
  // VisibleForTesting
  SchemaAndPrimaryKeyColumns unionizeSchemas(
      SchemaAndPrimaryKeyColumns firstSchema, SchemaAndPrimaryKeyColumns secondSchema) {
    Map<String, Field> firstSchemaFields = schemaFields(firstSchema.schema);
    Map<String, Field> secondSchemaFields = schemaFields(secondSchema.schema);
    Map<String, Field> unionizedSchemaFields = new LinkedHashMap<>();

    Set<String> primaryKeyColumns = new LinkedHashSet<>();
    primaryKeyColumns.addAll(firstSchema.getPrimaryKeyColumns());
    primaryKeyColumns.addAll(secondSchema.getPrimaryKeyColumns());

    firstSchemaFields.forEach((name, firstField) -> {
      Field secondField = secondSchemaFields.get(name);
      if (secondField == null) {
        // Repeated fields are implicitly nullable; no need to set a new mode for them
        if (!Field.Mode.REPEATED.equals(firstField.getMode())) {
          unionizedSchemaFields.put(name, firstField.toBuilder().setMode(Field.Mode.NULLABLE).build());
        } else {
          unionizedSchemaFields.put(name, firstField);
        }
      } else {
        unionizedSchemaFields.put(name, unionizeFields(firstField, secondField));
      }
    });

    maybeAddToUnionizedFields(secondSchemaFields, unionizedSchemaFields);

    return new SchemaAndPrimaryKeyColumns(
        com.google.cloud.bigquery.Schema.of(unionizedSchemaFields.values()),
        new ArrayList<>(primaryKeyColumns)
    );
  }

  private void maybeAddToUnionizedFields(Map<String, Field> secondSchemaFields,
                                         Map<String, Field> unionizedFields) {
    secondSchemaFields.forEach((name, secondField) -> {
      if (!unionizedFields.containsKey(name)) {
        if (Mode.REPEATED.equals(secondField.getMode())) {
          // Repeated fields are implicitly nullable; no need to set a new mode for them
          unionizedFields.put(name, secondField);
        } else {
          unionizedFields.put(name, secondField.toBuilder().setMode(Mode.NULLABLE).build());
        }
      }
    });
  }

  private void validateSchemaChange(
      com.google.cloud.bigquery.Schema existingSchema, com.google.cloud.bigquery.Schema proposedSchema) {
    logger.trace("Validating schema change. Existing schema: {}; proposed Schema: {}",
        existingSchema.toString(), proposedSchema.toString());
    Map<String, Field> earliestSchemaFields = schemaFields(existingSchema);
    Map<String, Field> proposedSchemaFields = schemaFields(proposedSchema);

    for (Map.Entry<String, Field> entry : proposedSchemaFields.entrySet()) {
      if (!earliestSchemaFields.containsKey(entry.getKey())) {
        if (!isValidFieldAddition(entry.getValue())) {
          throw new BigQueryConnectException("New Field found with the name " + entry.getValue().getName()
              + " Ensure that " + BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG + " is true and "
              + BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG
              + " is true if " + entry.getKey() + " has mode REQUIRED in order to update the Schema");
        }
      } else if (isFieldRelaxation(earliestSchemaFields.get(entry.getKey()), entry.getValue())) {
        if (!allowBqRequiredFieldRelaxation) {
          throw new BigQueryConnectException(entry.getKey() + " has mode REQUIRED. Set "
              + BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG
              + " to true, to change the mode to NULLABLE");
        }
      }
    }
  }

  private boolean isFieldRelaxation(Field currentField, Field proposedField) {
    return currentField.getMode().equals(Field.Mode.REQUIRED)
        && proposedField.getMode().equals(Field.Mode.NULLABLE);
  }

  private boolean isValidFieldAddition(Field newField) {
    return allowNewBqFields && (
        newField.getMode().equals(Field.Mode.NULLABLE)
            || newField.getMode().equals(Field.Mode.REPEATED)
            || (
                newField.getMode().equals(Field.Mode.REQUIRED)
                    && allowBqRequiredFieldRelaxation
            )
      );
  }

  private com.google.cloud.bigquery.Schema relaxFieldsWhereNecessary(
      com.google.cloud.bigquery.Schema existingSchema,
      com.google.cloud.bigquery.Schema proposedSchema) {
    Map<String, Field> existingSchemaFields = schemaFields(existingSchema);
    Map<String, Field> proposedSchemaFields = schemaFields(proposedSchema);
    List<Field> newSchemaFields = new ArrayList<>();
    for (Map.Entry<String, Field> entry : proposedSchemaFields.entrySet()) {
      if (!existingSchemaFields.containsKey(entry.getKey()) && !Field.Mode.REPEATED.equals(entry.getValue().getMode())) {
        newSchemaFields.add(entry.getValue().toBuilder().setMode(Field.Mode.NULLABLE).build());
      } else {
        newSchemaFields.add(entry.getValue());
      }
    }
    return com.google.cloud.bigquery.Schema.of(newSchemaFields);
  }

  /**
   * Returns a unionized table description from a set of sink records going to the same BigQuery table.
   *
   * @param records The records used to get the unionized table description
   * @return The resulting table description
   */
  @VisibleForTesting
  String getUnionizedTableDescription(List<SinkRecord> records) {
    String tableDescription = null;
    for (SinkRecord record : records) {
      Schema kafkaValueSchema = schemaRetriever.retrieveValueSchema(record);
      if (kafkaValueSchema == null) {
        continue;
      }
      tableDescription = kafkaValueSchema.doc() != null ? kafkaValueSchema.doc() : tableDescription;
    }
    return tableDescription;
  }

  private Map<String, Field> subFields(Field parent) {
    Map<String, Field> result = new LinkedHashMap<>();
    if (parent == null || parent.getSubFields() == null) {
      return result;
    }
    parent.getSubFields().forEach(field -> {
      if (field.getMode() == null) {
        field = field.toBuilder().setMode(Mode.NULLABLE).build();
      }
      result.put(field.getName().toLowerCase(), field);
    });
    return result;
  }

  /**
   * Returns a dictionary providing lookup of each field in the schema by name. The ordering of the
   * fields in the schema is preserved in the returned map.
   *
   * @param schema The BigQuery schema
   * @return A map allowing lookup of schema fields by name
   */
  private Map<String, Field> schemaFields(com.google.cloud.bigquery.Schema schema) {
    Map<String, Field> result = new LinkedHashMap<>();
    schema.getFields().forEach(field -> {
      if (field.getMode() == null) {
        field = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
      }
      result.put(field.getName().toLowerCase(), field);
    });
    return result;
  }

  // package private for testing.
  TableInfo constructTableInfo(
      TableId table,
      com.google.cloud.bigquery.Schema bigQuerySchema,
      List<String> primaryKeyColumns,
      String tableDescription,
      Boolean createSchema
  ) {
    StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema);

    if (intermediateTables) {
      // Shameful hack: make the table ingestion time-partitioned here so that the _PARTITIONTIME
      // pseudocolumn can be queried to filter out rows that are still in the streaming buffer
      builder.setTimePartitioning(TimePartitioning.of(Type.DAY));
    } else if (createSchema) {
      timePartitioningType.ifPresent(partitioningType -> {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.of(partitioningType).toBuilder();
        timestampPartitionFieldName.ifPresent(timePartitioningBuilder::setField);
        partitionExpiration.ifPresent(timePartitioningBuilder::setExpirationMs);

        builder.setTimePartitioning(timePartitioningBuilder.build());

        if (timestampPartitionFieldName.isPresent() && clusteringFieldName.isPresent()) {
          Clustering clustering = Clustering.newBuilder()
              .setFields(clusteringFieldName.get())
              .build();
          builder.setClustering(clustering);
        }

        if (kafkaKeyAsPrimaryKey) {
          assert Optional.of("").equals(kafkaKeyFieldName);

          builder.setTableConstraints(
              TableConstraints.newBuilder()
                  .setPrimaryKey(
                      PrimaryKey.newBuilder()
                          .setColumns(primaryKeyColumns)
                          .build()
                  ).build()
          );
        }
      });
    }

    StandardTableDefinition tableDefinition = builder.build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (intermediateTables) {
      tableInfoBuilder.setDescription("Temporary table");
    } else if (tableDescription != null) {
      tableInfoBuilder.setDescription(tableDescription);
    }

    return tableInfoBuilder.build();
  }

  private SchemaAndPrimaryKeyColumns getBigQuerySchema(Schema kafkaKeySchema, Schema kafkaValueSchema) {
    com.google.cloud.bigquery.Schema valueSchema = schemaConverter.convertSchema(kafkaValueSchema);

    return intermediateTables
        ? getIntermediateSchema(valueSchema, kafkaKeySchema)
        : getRegularSchema(valueSchema, kafkaKeySchema);
  }

  private SchemaAndPrimaryKeyColumns getIntermediateSchema(com.google.cloud.bigquery.Schema valueSchema, Schema kafkaKeySchema) {
    if (kafkaKeySchema == null) {
      throw new BigQueryConnectException(String.format(
          "Cannot create intermediate table without specifying a value for '%s'",
          BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG
      ));
    }

    List<Field> fields = new ArrayList<>();

    List<Field> valueFields = new ArrayList<>(valueSchema.getFields());
    if (kafkaDataFieldName.isPresent()) {
      String dataFieldName = sanitizeFieldNames
          ? FieldNameSanitizer.sanitizeName(kafkaDataFieldName.get())
          : kafkaDataFieldName.get();
      Field kafkaDataField = KafkaDataBuilder.buildKafkaDataField(dataFieldName);
      valueFields.add(kafkaDataField);
    }

    // Wrap the sink record value (and possibly also its Kafka data) in a struct in order to support deletes
    Field wrappedValueField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, LegacySQLTypeName.RECORD, valueFields.toArray(new Field[0]))
        .setMode(Field.Mode.NULLABLE)
        .build();
    fields.add(wrappedValueField);

    com.google.cloud.bigquery.Schema keySchema = schemaConverter.convertSchema(kafkaKeySchema);
    Field kafkaKeyField = Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, LegacySQLTypeName.RECORD, keySchema.getFields())
        .setMode(Field.Mode.REQUIRED)
        .build();
    fields.add(kafkaKeyField);

    Field iterationField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_ITERATION_FIELD_NAME, LegacySQLTypeName.INTEGER)
        .setMode(Field.Mode.REQUIRED)
        .build();
    fields.add(iterationField);

    Field partitionTimeField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
        .setMode(Field.Mode.NULLABLE)
        .build();
    fields.add(partitionTimeField);

    Field batchNumberField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER)
        .setMode(Field.Mode.REQUIRED)
        .build();
    fields.add(batchNumberField);

    return new SchemaAndPrimaryKeyColumns(
        com.google.cloud.bigquery.Schema.of(fields),
        Collections.emptyList()
    );
  }

  private SchemaAndPrimaryKeyColumns getRegularSchema(com.google.cloud.bigquery.Schema valueSchema, Schema kafkaKeySchema) {
    List<Field> fields = new ArrayList<>(valueSchema.getFields());
    List<String> primaryKeyColumns;

    if (kafkaDataFieldName.isPresent()) {
      String dataFieldName = sanitizeFieldNames
          ? FieldNameSanitizer.sanitizeName(kafkaDataFieldName.get())
          : kafkaDataFieldName.get();
      Field kafkaDataField = KafkaDataBuilder.buildKafkaDataField(dataFieldName);
      fields.add(kafkaDataField);
    }

    if (kafkaKeyFieldName.isPresent()) {
      com.google.cloud.bigquery.Schema keySchema = schemaConverter.convertSchema(kafkaKeySchema);
      if (kafkaKeyFieldName.get().isEmpty()) {
        // TODO: Gracefully handle collisions with value/TPO field names
        fields.addAll(keySchema.getFields());
        primaryKeyColumns = keySchema.getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());
      } else {
        String keyFieldName = sanitizeFieldNames
            ? FieldNameSanitizer.sanitizeName(kafkaKeyFieldName.get())
            : kafkaKeyFieldName.get();
        Field kafkaKeyField = Field.newBuilder(
            keyFieldName,
            LegacySQLTypeName.RECORD,
            keySchema.getFields()).setMode(Field.Mode.NULLABLE).build();
        fields.add(kafkaKeyField);
        primaryKeyColumns = Collections.emptyList();
      }
    } else {
      primaryKeyColumns = Collections.emptyList();
    }

    return new SchemaAndPrimaryKeyColumns(
        com.google.cloud.bigquery.Schema.of(fields),
        primaryKeyColumns
    );
  }

  private String table(TableId table) {
    return intermediateTables
        ? TableNameUtils.intTable(table)
        : TableNameUtils.table(table);
  }

  private SchemaAndPrimaryKeyColumns readTableSchema(TableId table) {
    logger.trace("Reading schema for {}", table(table));
    return Optional.ofNullable(bigQuery.getTable(table))
        .map(SchemaAndPrimaryKeyColumns::of)
        .orElse(null);
  }

  private Object lock(ConcurrentMap<TableId, Object> locks, TableId table) {
    return locks.computeIfAbsent(table, t -> new Object());
  }

  static class SchemaAndPrimaryKeyColumns {

    private final com.google.cloud.bigquery.Schema schema;
    private final List<String> primaryKeyColumns;

    public static SchemaAndPrimaryKeyColumns of(Table table) {
      return of(table.getDefinition(), table.getTableConstraints());
    }

    public static SchemaAndPrimaryKeyColumns of(TableInfo tableInfo) {
      return of(tableInfo.getDefinition(), tableInfo.getTableConstraints());
    }

    private static SchemaAndPrimaryKeyColumns of(TableDefinition tableDefinition, TableConstraints tableConstraints) {
      com.google.cloud.bigquery.Schema schema = tableDefinition.getSchema();

      List<String> primaryKeyColumns = Optional.ofNullable(tableConstraints)
          .map(TableConstraints::getPrimaryKey)
          .map(PrimaryKey::getColumns)
          .orElseGet(Collections::emptyList);

      return new SchemaAndPrimaryKeyColumns(schema, primaryKeyColumns);
    }

    public SchemaAndPrimaryKeyColumns(com.google.cloud.bigquery.Schema schema, List<String> primaryKeyColumns) {
      this.schema = schema;
      this.primaryKeyColumns = primaryKeyColumns;
    }

    public com.google.cloud.bigquery.Schema getSchema() {
      return schema;
    }

    public List<String> getPrimaryKeyColumns() {
      return primaryKeyColumns;
    }
  }

}
