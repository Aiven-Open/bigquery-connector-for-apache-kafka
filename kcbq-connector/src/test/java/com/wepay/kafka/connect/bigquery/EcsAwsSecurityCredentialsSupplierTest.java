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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.auth.oauth2.AwsSecurityCredentials;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class EcsAwsSecurityCredentialsSupplierTest {

  private static final String RELATIVE_URI = "/v2/credentials/token-abc";

  private HttpServer server;

  /** Counts requests the stub actually served, so tests can assert how many attempts were made. */
  private final AtomicInteger requestCount = new AtomicInteger();

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  /** Starts a stub container-credentials endpoint returning the given status and body. */
  private String startStub(int status, String body) throws IOException {
    return startStub(new Response(status, body));
  }

  /**
   * Starts a stub that serves the given responses in order, one per request, repeating the last
   * once they run out. Lets a test make an early attempt fail and a later one succeed.
   */
  private String startStub(Response... responses) throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        RELATIVE_URI,
        exchange -> {
          int index = requestCount.getAndIncrement();
          Response response = responses[Math.min(index, responses.length - 1)];
          byte[] bytes = response.body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(response.status, bytes.length == 0 ? -1 : bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        });
    server.start();
    return "http://localhost:" + server.getAddress().getPort();
  }

  /** One canned stub response. */
  private static final class Response {
    private final int status;
    private final String body;

    Response(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }

  /**
   * Supplier with an injected env map, so tests never mutate the real process environment, and a
   * recording {@code sleep} so retry tests assert on backoff without actually waiting.
   */
  private static class RecordingSupplier extends EcsAwsSecurityCredentialsSupplier {
    private static final long serialVersionUID = 1L;

    final List<Long> sleeps = new ArrayList<>();
    private final Map<String, String> env;

    RecordingSupplier(String baseUri, Map<String, String> env) {
      super(baseUri);
      this.env = env;
    }

    @Override
    String getEnv(String name) {
      return env.get(name);
    }

    @Override
    void sleep(long millis) throws InterruptedException {
      sleeps.add(millis);
    }
  }

  private static RecordingSupplier supplier(String baseUri, Map<String, String> env) {
    return new RecordingSupplier(baseUri, env);
  }

  private static String deadBaseUri() throws IOException {
    int deadPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      deadPort = socket.getLocalPort();
    }
    return "http://localhost:" + deadPort;
  }

  private static Map<String, String> envWithRelativeUri() {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.RELATIVE_URI_ENV, RELATIVE_URI);
    return env;
  }

  @Test
  public void getCredentialsReturnsParsedCredentials() throws IOException {
    String baseUri =
        startStub(
            200, "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\",\"Token\":\"TOKEN\"}");
    AwsSecurityCredentials creds = supplier(baseUri, envWithRelativeUri()).getCredentials(null);
    assertEquals("AKID", creds.getAccessKeyId());
    assertEquals("SECRET", creds.getSecretAccessKey());
    assertEquals("TOKEN", creds.getSessionToken());
  }

  @Test
  public void getCredentialsThrowsOnNon200() throws IOException {
    String baseUri = startStub(500, "boom");
    assertThrows(
        IOException.class, () -> supplier(baseUri, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsWhenRelativeUriEnvMissing() {
    assertThrows(
        IOException.class,
        () -> supplier("http://localhost:1", new HashMap<>()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsWhenTokenMissing() throws IOException {
    String baseUri = startStub(200, "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\"}");
    assertThrows(
        IOException.class, () -> supplier(baseUri, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsOnMalformedJson() throws IOException {
    String baseUri = startStub(200, "not json");
    assertThrows(
        IOException.class, () -> supplier(baseUri, envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getCredentialsThrowsOnConnectionRefused() throws IOException {
    // A port with nothing listening: the fetch must surface as an IOException.
    assertThrows(
        IOException.class,
        () -> supplier(deadBaseUri(), envWithRelativeUri()).getCredentials(null));
  }

  @Test
  public void getRegionPrefersAwsRegion() throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.REGION_ENV, "us-east-1");
    env.put(EcsAwsSecurityCredentialsSupplier.DEFAULT_REGION_ENV, "eu-west-1");
    assertEquals("us-east-1", supplier("http://unused", env).getRegion(null));
  }

  @Test
  public void getRegionFallsBackToDefaultRegion() throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.DEFAULT_REGION_ENV, "eu-west-1");
    assertEquals("eu-west-1", supplier("http://unused", env).getRegion(null));
  }

  @Test
  public void getRegionThrowsWhenUnset() {
    assertThrows(
        IOException.class, () -> supplier("http://unused", new HashMap<>()).getRegion(null));
  }

  @Test
  public void getRegionTrimsWhitespace() throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put(EcsAwsSecurityCredentialsSupplier.REGION_ENV, "  us-east-1  ");
    assertEquals("us-east-1", supplier("http://unused", env).getRegion(null));
  }

  @Test
  public void getCredentialsRetriesAndSucceedsAfterTransientServerError() throws IOException {
    String baseUri =
        startStub(
            new Response(503, "unavailable"),
            new Response(
                200,
                "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\",\"Token\":\"TOKEN\"}"));
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    AwsSecurityCredentials creds = supplier.getCredentials(null);

    assertEquals("AKID", creds.getAccessKeyId());
    assertEquals(2, requestCount.get(), "should have retried exactly once");
    assertEquals(1, supplier.sleeps.size(), "should have backed off once");
  }

  @Test
  public void getCredentialsRetriesOnTooManyRequests() throws IOException {
    String baseUri =
        startStub(
            new Response(429, "slow down"),
            new Response(
                200,
                "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\",\"Token\":\"TOKEN\"}"));
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    assertEquals("AKID", supplier.getCredentials(null).getAccessKeyId());
    assertEquals(2, requestCount.get());
  }

  @Test
  public void getCredentialsExhaustsRetriesOnPersistentServerError() throws IOException {
    String baseUri = startStub(500, "boom");
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    IOException e = assertThrows(IOException.class, () -> supplier.getCredentials(null));

    int expectedAttempts = EcsAwsSecurityCredentialsSupplier.MAX_RETRIES + 1;
    assertEquals(expectedAttempts, requestCount.get());
    assertEquals(EcsAwsSecurityCredentialsSupplier.MAX_RETRIES, supplier.sleeps.size());
    assertTrue(
        e.getMessage().contains("after " + expectedAttempts + " attempts"),
        "unexpected message: " + e.getMessage());
    assertTrue(e.getCause() instanceof IOException, "should carry the last failure as its cause");
  }

  @Test
  public void getCredentialsRetriesWhenEndpointUnreachable() throws IOException {
    RecordingSupplier supplier = supplier(deadBaseUri(), envWithRelativeUri());

    assertThrows(IOException.class, () -> supplier.getCredentials(null));

    assertEquals(EcsAwsSecurityCredentialsSupplier.MAX_RETRIES, supplier.sleeps.size());
  }

  @Test
  public void getCredentialsDoesNotRetryOnForbidden() throws IOException {
    // 403 means a misconfigured task role, not a transient fault: fail on the first attempt.
    String baseUri = startStub(403, "denied");
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    assertThrows(IOException.class, () -> supplier.getCredentials(null));

    assertEquals(1, requestCount.get());
    assertEquals(0, supplier.sleeps.size());
  }

  @Test
  public void getCredentialsDoesNotRetryOnMalformedJson() throws IOException {
    String baseUri = startStub(200, "not json");
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    assertThrows(IOException.class, () -> supplier.getCredentials(null));

    assertEquals(1, requestCount.get());
    assertEquals(0, supplier.sleeps.size());
  }

  @Test
  public void getCredentialsDoesNotRetryWhenTokenMissing() throws IOException {
    String baseUri = startStub(200, "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"SECRET\"}");
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    assertThrows(IOException.class, () -> supplier.getCredentials(null));

    assertEquals(1, requestCount.get());
    assertEquals(0, supplier.sleeps.size());
  }

  @Test
  public void getCredentialsDoesNotRetryWhenRelativeUriEnvMissing() {
    RecordingSupplier supplier = supplier("http://localhost:1", new HashMap<>());

    assertThrows(IOException.class, () -> supplier.getCredentials(null));

    assertEquals(0, supplier.sleeps.size(), "a missing env var is not a transient fault");
  }

  @Test
  public void backoffStaysWithinFullJitterBounds() throws IOException {
    String baseUri = startStub(500, "boom");
    RecordingSupplier supplier = supplier(baseUri, envWithRelativeUri());

    assertThrows(IOException.class, () -> supplier.getCredentials(null));

    for (int attempt = 0; attempt < supplier.sleeps.size(); attempt++) {
      long bound =
          Math.min(
              EcsAwsSecurityCredentialsSupplier.MAX_BACKOFF_MS,
              (long) EcsAwsSecurityCredentialsSupplier.BASE_BACKOFF_MS << attempt);
      long delay = supplier.sleeps.get(attempt);
      assertTrue(
          delay >= 0 && delay <= bound,
          "delay " + delay + " outside [0, " + bound + "] for attempt " + attempt);
    }
  }

  @Test
  public void getCredentialsRestoresInterruptFlagWhenInterruptedWhileBackingOff()
      throws IOException {
    String baseUri = startStub(500, "boom");
    EcsAwsSecurityCredentialsSupplier supplier =
        new EcsAwsSecurityCredentialsSupplier(baseUri) {
          @Override
          String getEnv(String name) {
            return envWithRelativeUri().get(name);
          }

          @Override
          void sleep(long millis) throws InterruptedException {
            throw new InterruptedException("interrupted during backoff");
          }
        };

    IOException e = assertThrows(IOException.class, () -> supplier.getCredentials(null));

    assertTrue(e.getCause() instanceof InterruptedException);
    // Thread.interrupted() both asserts and clears, so the flag does not leak to other tests.
    assertTrue(Thread.interrupted(), "interrupt flag should have been restored");
    assertEquals(1, requestCount.get(), "must not keep retrying after an interrupt");
  }
}
