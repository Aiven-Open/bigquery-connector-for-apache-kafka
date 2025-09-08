
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.wepay.kafka.connect.bigquery.config;

import io.aiven.kafka.config.tools.BaseConfigDefBean;
import io.aiven.kafka.config.tools.ExtendedConfigKeyBean;
import org.apache.velocity.tools.config.DefaultKey;
import org.apache.velocity.tools.config.ValidScope;

/**
 * The Velocity RAT plugin that provides access to the RAT data.
 * <p>
 * DEVHINT: Be careful when removing methods as this may invalidate contents and functionality of the velocity templates.
 * </p>
 */
@SuppressWarnings("unused")
@DefaultKey("extendedConfigDef")
@ValidScope({"application"})
public class BigQueryConfigDefBean extends BaseConfigDefBean<ExtendedConfigKeyBean> {
  /**
   * Constructor.
   */
  public BigQueryConfigDefBean() {
    super(BigQuerySinkConfig.getConfig(), ExtendedConfigKeyBean::new);
  }
}