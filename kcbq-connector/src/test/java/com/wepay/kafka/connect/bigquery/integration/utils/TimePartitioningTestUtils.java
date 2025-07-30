/*
 * Copyright 2025 Aiven Oy and bigquery-connector-for-apache-kafka project contributors
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

package com.wepay.kafka.connect.bigquery.integration.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimePartitioningTestUtils {

    public static void produceRecordsWithTimestamps(
            EmbeddedConnectCluster connect,
            String topic,
            long numRecords,
            long baseTimestamp,
            TimePartitioning.Type type,
            Converter converter
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());

        try (Producer<Void, String> producer = new KafkaProducer<>(props, Serdes.Void().serializer(), Serdes.String().serializer())) {
            for (int i = 0; i < numRecords; i++) {
                long timestamp = computeTimestamp(type, baseTimestamp, (i % 3) - 1);
                String value = getValue(converter, topic, i);
                producer.send(new ProducerRecord<>(topic, null, timestamp, null, value)).get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new ConnectException("Failed to produce timestamped records", e);
        }
    }

    private static String getValue(Converter converter, String topic, long iteration) {
        final Schema schema = SchemaBuilder.struct()
                .optional()
                .field("i", Schema.INT64_SCHEMA)
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.BOOLEAN_SCHEMA)
                .field("f3", Schema.FLOAT64_SCHEMA)
                .build();

        final Struct struct = new Struct(schema)
                .put("i", iteration)
                .put("f1", iteration % 2 == 0 ? "a string" : "another string")
                .put("f2", iteration % 3 == 0)
                .put("f3", iteration / 39.80);

        return new String(converter.fromConnectData(topic, schema, struct));
    }

    public static long computeTimestamp(TimePartitioning.Type partitioningType, long testStartTime, long shiftAmount) {
        long partitionDelta;
        switch (partitioningType) {
            case HOUR:
                partitionDelta = TimeUnit.HOURS.toMillis(1);
                break;
            case DAY:
                partitionDelta = TimeUnit.DAYS.toMillis(1);
                break;
            case MONTH:
                partitionDelta = TimeUnit.DAYS.toMillis(31);
                break;
            case YEAR:
                partitionDelta = TimeUnit.DAYS.toMillis(366);
                break;
            default:
                throw new ConnectException("Unexpected partitioning type: " + partitioningType);
        }

        return testStartTime + (shiftAmount * partitionDelta);
    }

    public static void assertPartitionContainsData(BigQuery bigQuery, String dataset, String table, TimePartitioning.Type type, long timestampMillis) throws InterruptedException {
        String query = String.format(
                "SELECT * FROM `%s`.`%s` WHERE _PARTITIONTIME = TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(%d), %s)",
                dataset,
                table,
                timestampMillis,
                type.name()
        );
        TableResult tableResult = bigQuery.query(QueryJobConfiguration.of(query));

        assertTrue(
                tableResult.getValues().iterator().hasNext(),
                "Expected records in partition for timestamp: " + timestampMillis
        );
    }
}
