/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.paas.dashboard.dto.pulsar;

import com.paas.dashboard.util.config.BaseConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class PulsarInstanceDto extends BaseConfig {

    private String id;
    private String host;
    private int port;
    private String functionHost;
    private int functionPort;
    private boolean enableTls;
    private boolean functionEnableTls;
    private String caFile;
    private String clientCertFile;
    private String clientKeyFile;
    private String clientKeyPassword;

    public PulsarInstanceDto(String id,
                             String name,
                             String host,
                             int port,
                             String functionHost,
                             int functionPort,
                             boolean enableTls,
                             boolean functionEnableTls,
                             String caFile,
                             String clientCertFile,
                             String clientKeyFile,
                             String clientKeyPassword
    ) {
        super(id, name);
        this.id = id;
        this.host = host;
        this.port = port;
        this.functionHost = functionHost;
        this.functionPort = functionPort;
        this.enableTls = enableTls;
        this.functionEnableTls = functionEnableTls;
        this.caFile = caFile;
        this.clientCertFile = clientCertFile;
        this.clientKeyFile = clientKeyFile;
        this.clientKeyPassword = clientKeyPassword;
    }
}
