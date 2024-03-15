/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.integration.utils;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCaseLogger extends TestWatcher {
  private static final Logger logger = LoggerFactory.getLogger(TestCaseLogger.class);

  @Override
  protected void starting(Description description) {
    super.starting(description);
    logger.info("Starting test {}", description.getMethodName());
  }

  @Override
  protected void finished(Description description) {
    super.finished(description);
    logger.info("Finished test {}", description.getMethodName());
  }

}
