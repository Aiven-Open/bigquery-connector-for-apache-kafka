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

package com.wepay.kafka.connect.bigquery.integration.utils;

import static io.confluent.kafka.schemaregistry.ClusterTestHarness.KAFKASTORE_TOPIC;
import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRegistryTestUtils {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryTestUtils.class);

  protected String bootstrapServers;

  private String schemaRegistryUrl;

  private RestApp restApp;

  public SchemaRegistryTestUtils(String bootstrapServers) {
    this.bootstrapServers = requireNonNull(bootstrapServers);
  }

  public void start() throws Exception {
    int port = findAvailableOpenPort();
    restApp = new RestApp(port, null, this.bootstrapServers,
        KAFKASTORE_TOPIC, CompatibilityLevel.NONE.name, true, new Properties());
    restApp.start();

    TestUtils.waitForCondition(() -> restApp.restServer.isRunning(), 10000L,
        "Schema Registry start timed out.");

    schemaRegistryUrl = restApp.restServer.getURI().toString();
  }

  public void stop() throws Exception {
    restApp.stop();
  }

  public String schemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  private Integer findAvailableOpenPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }


  private KafkaProducer<byte[], byte[]> configureProducer() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 10);
    return new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
  }


  public void produceRecords(
      Converter converter,
      List<SchemaAndValue> recordsList,
      String topic
  ) {
    try (KafkaProducer<byte[], byte[]> producer = configureProducer()) {
      for (int i = 0; i < recordsList.size(); i++) {
        SchemaAndValue schemaAndValue = recordsList.get(i);
        byte[] convertedStruct = converter.fromConnectData(topic, schemaAndValue.schema(), schemaAndValue.value());
        ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, 0, String.valueOf(i).getBytes(), convertedStruct);
        try {
          producer.send(msg).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new KafkaException("Could not produce message: " + msg, e);
        }
      }
    }
  }

  public void produceRecordsWithKey(
      Converter keyConverter,
      Converter valueConverter,
      List<List<SchemaAndValue>> recordsList,
      String topic
  ) {
    int iterationLogThreshold = Math.min(10_000, (int) Math.ceil(recordsList.size() / 100.0));
    try (KafkaProducer<byte[], byte[]> producer = configureProducer()) {
      List<Future<RecordMetadata>> produceFutures = new ArrayList<>();
      for (int i = 0; i < recordsList.size(); i++) {
        List<SchemaAndValue> record = recordsList.get(i);
        SchemaAndValue key = record.get(0);
        SchemaAndValue value = record.get(1);
        byte[] convertedStructKey = keyConverter.fromConnectData(topic, key.schema(), key.value());
        byte[] convertedStructValue;
        if (value == null) {
          convertedStructValue = valueConverter.fromConnectData(topic, null, null);
        } else {
          convertedStructValue = valueConverter.fromConnectData(topic, value.schema(), value.value());
        }
        ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, convertedStructKey, convertedStructValue);
        final int iteration = i + 1;
        Future<RecordMetadata> produceFuture = producer.send(
            msg,
            (recordMetadata, error) -> {
              if (error != null)
                return;

              if (iteration % iterationLogThreshold == 0)
                log.info("Sent {} Avro records to topic {}", iteration, topic);
            }
        );
        produceFutures.add(produceFuture);
      }
      for (int i = 0; i < produceFutures.size(); i++) {
        Future<RecordMetadata> produceFuture = produceFutures.get(i);
        try {
          produceFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
          SchemaAndValue recordKey = recordsList.get(i).get(0);
          SchemaAndValue recordValue = recordsList.get(i).get(1);
          throw new KafkaException(
              "Could not produce message " + i
                  + "  with key " + recordKey
                  + " and value " + recordValue,
              e
          );
        }
      }
    }
  }

}
