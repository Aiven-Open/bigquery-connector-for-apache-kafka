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

package com.wepay.kafka.connect.bigquery.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Gson utilities for safe JSON handling on Java 9+.
 *
 * <p>Exposes a preconfigured {@link Gson} instance that registers a hierarchy adapter for {@link ByteBuffer}
 * , ensuring serialization does not rely on illegal reflection into JDK internals (e.g.,
 * ByteBuffer#hb) which would otherwise throw {@code InaccessibleObjectException} on Java 9+.
 */
public final class GsonUtils {

  /** A ready-to-use Gson that safely serializes ByteBuffer (as Base64 strings). */
  public static final Gson SAFE_GSON =
      new GsonBuilder()
          // Use hierarchy adapter so HeapByteBuffer/DirectByteBuffer subclasses are covered.
          .registerTypeHierarchyAdapter(ByteBuffer.class, new ByteBufferTypeAdapter())
          .create();

  private GsonUtils() {
    // no instances
  }

  /**
   * Serializes {@link ByteBuffer} values as Base64 strings and deserializes them back. Registered
   * as a hierarchy adapter so it handles all ByteBuffer subclasses.
   */
  static final class ByteBufferTypeAdapter extends TypeAdapter<ByteBuffer> {

    @Override
    public void write(JsonWriter out, ByteBuffer value) throws IOException {
      if (value == null) {
        out.nullValue();
        return;
      }
      // Duplicate to avoid mutating the original buffer's position/limit.
      ByteBuffer dup = value.duplicate();
      byte[] bytes = new byte[dup.remaining()];
      dup.get(bytes);
      out.value(Base64.getEncoder().encodeToString(bytes));
    }

    @Override
    public ByteBuffer read(JsonReader in) throws IOException {
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
        return null;
      }
      byte[] bytes = Base64.getDecoder().decode(in.nextString());
      return ByteBuffer.wrap(bytes);
    }
  }
}
