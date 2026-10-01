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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.auth.oauth2.AwsCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

public class GcpClientBuilderWifTest {

  private static final String AWS_SUBJECT_TOKEN_TYPE =
      "urn:ietf:params:aws:token-type:aws4_request";
  private static final String AUDIENCE =
      "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/aws";
  private static final String TOKEN_URL = "https://sts.googleapis.com/v1/token";
  private static final String IMPERSONATION_URL =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
          + "sa@proj.iam.gserviceaccount.com:generateAccessToken";
  private static final String REGIONAL_URL =
      "https://sts.us-east-1.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15";

  private static JsonObject baseAwsConfig() {
    JsonObject o = new JsonObject();
    o.addProperty("type", "external_account");
    o.addProperty("audience", AUDIENCE);
    o.addProperty("subject_token_type", AWS_SUBJECT_TOKEN_TYPE);
    o.addProperty("token_url", TOKEN_URL);
    o.addProperty("service_account_impersonation_url", IMPERSONATION_URL);
    return o;
  }

  /** Invokes the private {@code credentials()} build path, unwrapping build-time exceptions. */
  private static GoogleCredentials credentials(String keyJson, boolean useStorageWriteApi) {
    GcpClientBuilder<?> builder =
        new GcpClientBuilder.BigQueryBuilder()
            .withProject("test-project")
            .withKeySource(GcpClientBuilder.KeySource.WIF_JSON)
            .withKey(keyJson)
            .withWriterApi(useStorageWriteApi);
    try {
      Method m = GcpClientBuilder.class.getDeclaredMethod("credentials");
      m.setAccessible(true);
      return (GoogleCredentials) m.invoke(builder);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void awsKeyfileBuildsAwsCredentialsWithExpectedFields() {
    AwsCredentials aws =
        assertInstanceOf(AwsCredentials.class, credentials(baseAwsConfig().toString(), false));
    assertEquals(AUDIENCE, aws.getAudience());
    assertEquals(AWS_SUBJECT_TOKEN_TYPE, aws.getSubjectTokenType());
    assertEquals(TOKEN_URL, aws.getTokenUrl());
    assertEquals(IMPERSONATION_URL, aws.getServiceAccountImpersonationUrl());
  }

  @Test
  public void awsKeyfileWiresEcsSupplier() throws Exception {
    AwsCredentials aws = (AwsCredentials) credentials(baseAwsConfig().toString(), false);
    Field f = AwsCredentials.class.getDeclaredField("awsSecurityCredentialsSupplier");
    f.setAccessible(true);
    assertInstanceOf(EcsAwsSecurityCredentialsSupplier.class, f.get(aws));
  }

  @Test
  public void storageWriteApiSetsConnectorScopes() throws Exception {
    AwsCredentials aws = (AwsCredentials) credentials(baseAwsConfig().toString(), true);
    Collection<String> scopes = aws.getScopes();
    assertNotNull(scopes);

    Field scopesField = GcpClientBuilder.class.getDeclaredField("scopes");
    scopesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Collection<String> connectorScopes = (Collection<String>) scopesField.get(null);
    assertEquals(new HashSet<>(connectorScopes), new HashSet<>(scopes));
  }

  @Test
  public void credentialSourceStrippedButRegionalUrlApplied() {
    JsonObject config = baseAwsConfig();
    JsonObject credSource = new JsonObject();
    credSource.addProperty("environment_id", "aws1");
    credSource.addProperty("regional_cred_verification_url", REGIONAL_URL);
    config.add("credential_source", credSource);

    AwsCredentials aws = (AwsCredentials) credentials(config.toString(), false);
    assertEquals(REGIONAL_URL, aws.getRegionalCredentialVerificationUrlOverride());
  }

  @Test
  public void nonAwsSubjectTokenTypeFailsFast() {
    JsonObject config = baseAwsConfig();
    config.addProperty("subject_token_type", "urn:ietf:params:oauth:token-type:jwt");
    BigQueryConnectException e =
        assertThrows(BigQueryConnectException.class, () -> credentials(config.toString(), false));
    assertMessageContains(e, "supports only AWS");
  }

  @Test
  public void missingAudienceFailsFast() {
    JsonObject config = baseAwsConfig();
    config.remove("audience");
    BigQueryConnectException e =
        assertThrows(BigQueryConnectException.class, () -> credentials(config.toString(), false));
    assertMessageContains(e, "must define audience and token_url");
  }

  @Test
  public void malformedKeyfileFailsFast() {
    BigQueryConnectException e =
        assertThrows(BigQueryConnectException.class, () -> credentials("{{", false));
    assertMessageContains(e, "not valid JSON");
  }

  @Test
  public void emptyConfigFailsFast() {
    // An empty keyfile parses to a null object, caught by the empty/invalid guard.
    BigQueryConnectException e =
        assertThrows(BigQueryConnectException.class, () -> credentials("", false));
    assertMessageContains(e, "empty or not valid JSON");
  }

  @Test
  public void invalidTokenUrlFailsValidation() {
    JsonObject config = baseAwsConfig();
    config.addProperty("token_url", "https://evil.example.com/token");
    // GCPValidator rejects a non-Google token_url with IllegalArgumentException; matching the
    // existing JSON/FILE branches, it is not wrapped in BigQueryConnectException.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> credentials(config.toString(), false));
    assertMessageContains(e, "token_url");
  }

  private static void assertMessageContains(Throwable t, String substring) {
    String message = t.getMessage();
    assertNotNull(message, "exception had no message");
    assertTrue(message.contains(substring), message);
  }
}
