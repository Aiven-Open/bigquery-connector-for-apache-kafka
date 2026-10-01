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

import static com.wepay.kafka.connect.bigquery.utils.GsonUtils.getAsString;

import com.google.auth.oauth2.AwsSecurityCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountSupplierContext;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.wepay.kafka.connect.bigquery.utils.Time;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AwsSecurityCredentialsSupplier} that sources AWS credentials from the ECS/Fargate
 * container credentials endpoint (the relative-URI form).
 *
 * <p>google-auth's built-in AWS provider reads credentials only from environment variables or the
 * EC2 IMDS endpoint; it does not read the ECS/Fargate container-credentials endpoint ({@code
 * 169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI}), which is the only place Fargate delivers
 * task-role credentials. This supplier plugs that gap for Workload Identity Federation ({@code
 * keySource=WIF_JSON}) without pulling in the AWS SDK and without setting any process-wide
 * environment variables.
 *
 * <p>The supplier is stateless and fetches on demand (no caching): the ECS agent keeps the
 * endpoint's credentials valid and rotates them underneath, and google-auth caches the derived GCP
 * token and only calls back on refresh (~hourly). It is therefore thread-safe with no
 * synchronization; the shared {@link HttpClient} and {@link Gson} are {@code static} (both are safe
 * to share for reads) which also keeps the class cheap to serialize.
 */
public class EcsAwsSecurityCredentialsSupplier implements AwsSecurityCredentialsSupplier {

  private static final Logger logger =
      LoggerFactory.getLogger(EcsAwsSecurityCredentialsSupplier.class);

  private static final long serialVersionUID = 1L;

  /** Default base URI for the ECS/Fargate container credentials endpoint. */
  static final String DEFAULT_BASE_URI = "http://169.254.170.2";

  static final String RELATIVE_URI_ENV = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
  static final String REGION_ENV = "AWS_REGION";
  static final String DEFAULT_REGION_ENV = "AWS_DEFAULT_REGION";

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

  /** Retries after the initial attempt, so the endpoint is called at most MAX_RETRIES + 1 times. */
  static final int MAX_RETRIES = 3;

  static final int BASE_BACKOFF_MS = 1000;
  static final int MAX_BACKOFF_MS = 8000;

  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
  private static final Gson GSON = new Gson();

  private final String baseUri;

  public EcsAwsSecurityCredentialsSupplier() {
    this(DEFAULT_BASE_URI);
  }

  /**
   * @param baseUri base URI of the container credentials endpoint; primarily a seam for tests to
   *     point at a local stub. Production uses {@link #DEFAULT_BASE_URI}.
   */
  EcsAwsSecurityCredentialsSupplier(String baseUri) {
    this.baseUri = baseUri;
  }

  @Override
  public String getRegion(ExternalAccountSupplierContext context) throws IOException {
    String source = REGION_ENV;
    String region = trimToNull(getEnv(REGION_ENV));
    if (region == null) {
      source = DEFAULT_REGION_ENV;
      region = trimToNull(getEnv(DEFAULT_REGION_ENV));
    }
    if (region == null) {
      throw new IOException(
          "Could not resolve AWS region: set "
              + REGION_ENV
              + " or "
              + DEFAULT_REGION_ENV
              + " (it is baked into the STS request signature, so there is no safe default).");
    }
    logger.debug("Resolved AWS region {} from {}", region, source);
    return region;
  }

  @Override
  public AwsSecurityCredentials getCredentials(ExternalAccountSupplierContext context)
      throws IOException {
    String relativeUri = trimToNull(getEnv(RELATIVE_URI_ENV));
    if (relativeUri == null) {
      throw new IOException(
          "Environment variable "
              + RELATIVE_URI_ENV
              + " is not set; the ECS/Fargate container "
              + "credentials endpoint is unavailable (is this running on ECS/Fargate?).");
    }

    try {
      // attempt 0 is the initial try, so MAX_RETRIES retries means MAX_RETRIES + 1 attempts.
      for (int attempt = 0; ; attempt++) {
        try {
          return fetchOnce(relativeUri);
        } catch (RetryableIoException e) {
          if (attempt == MAX_RETRIES) {
            throw new IOException(
                "Failed to fetch AWS container credentials after "
                    + (MAX_RETRIES + 1)
                    + " attempts",
                e);
          }
          long delayMs = jitteredBackoffMillis(attempt);
          logger.warn(
              "Attempt {} of {} to fetch AWS container credentials failed, retrying in {} ms: {}",
              attempt + 1,
              MAX_RETRIES + 1,
              delayMs,
              e.getMessage());
          sleep(delayMs);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching AWS container credentials", e);
    }
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private AwsSecurityCredentials fetchOnce(String relativeUri)
      throws IOException, InterruptedException {
    // Log the base URI only; the relative URI carries a per-task credential path token.
    logger.debug("Fetching AWS container credentials from ECS/Fargate endpoint {}", baseUri);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUri + relativeUri))
            .timeout(REQUEST_TIMEOUT)
            .GET()
            .build();

    HttpResponse<String> response;

    try {
      response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      // Connect refused / timeout / reset: the ECS agent is momentarily unavailable.
      throw new RetryableIoException(
          "Failed to reach the ECS/Fargate container credentials endpoint " + baseUri, e);
    }

    int status = response.statusCode();
    if (status != 200) {
      // The body is deliberately left out: this endpoint serves credential material.
      String message = "Failed to fetch AWS container credentials: HTTP " + status;
      if (isRetryableStatus(status)) {
        throw new RetryableIoException(message);
      }
      throw new IOException(message);
    }

    JsonObject json;
    try {
      json = GSON.fromJson(response.body(), JsonObject.class);
    } catch (JsonSyntaxException e) {
      throw new IOException("AWS container credentials response was not valid JSON", e);
    }
    if (json == null) {
      throw new IOException("AWS container credentials response was not valid JSON");
    }
    String accessKeyId = getAsString(json, "AccessKeyId");
    String secretAccessKey = getAsString(json, "SecretAccessKey");
    String token = getAsString(json, "Token");
    // Token is required: this endpoint only serves temporary role credentials, and google-auth
    // needs it to SigV4-sign GetCallerIdentity. Fail here rather than opaquely inside STS.
    if (accessKeyId == null || secretAccessKey == null || token == null) {
      throw new IOException(
          "AWS container credentials response missing AccessKeyId/SecretAccessKey/Token");
    }
    logger.debug("Obtained temporary AWS credentials from ECS/Fargate container endpoint");
    return new AwsSecurityCredentials(accessKeyId, secretAccessKey, token);
  }

  /**
   * Reads an environment variable. Package-private and overridable so tests can supply values
   * without mutating the real process environment.
   */
  String getEnv(String name) {
    return System.getenv(name);
  }

  /**
   * Whether an HTTP status from the container credentials endpoint is worth retrying. 5xx and 429
   * are transient agent-side conditions; other 4xx (e.g. 403/404) mean a misconfigured task role or
   * path and are not retried.
   */
  private static boolean isRetryableStatus(int status) {
    return status >= 500 || status == 429;
  }

  /** Full jitter: a uniform draw from [0, min(cap, base * 2^attempt)]. */
  private static long jitteredBackoffMillis(int attempt) {
    long bound = Math.min(MAX_BACKOFF_MS, (long) BASE_BACKOFF_MS << attempt);
    return ThreadLocalRandom.current().nextLong(bound + 1);
  }

  /**
   * Sleeps between retries. Package-private and overridable so tests can record delays without
   * actually waiting.
   */
  void sleep(long millis) throws InterruptedException {
    Time.SYSTEM.sleep(millis);
  }

  /** Marks a fetch failure that is worth retrying (transient endpoint/agent problem). */
  private static final class RetryableIoException extends IOException {
    private static final long serialVersionUID = 1L;

    RetryableIoException(String message) {
      super(message);
    }

    RetryableIoException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
