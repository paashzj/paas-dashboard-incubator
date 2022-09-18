/**
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

package com.paas.dashboard.service;

import com.paas.dashboard.dto.PulsarReqDto;
import com.paas.dashboard.dto.TlsContextDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class PulsarAdminService {

    private final Map<Integer, PulsarAdmin> pulsarAdminMap = new HashMap<>();

    public List<String> fetchTenants(PulsarReqDto pulsarReqDto) throws Exception {
        PulsarAdmin pulsarAdmin = getPulsarAdmin(pulsarReqDto.getServiceUrl(),
                pulsarReqDto.getId(), pulsarReqDto.getTlsContextDto());
        return pulsarAdmin.tenants().getTenants();
    }

    public TenantInfo getTenantInfo(PulsarReqDto pulsarReqDto, String tenantName) throws Exception {
        PulsarAdmin pulsarAdmin = getPulsarAdmin(pulsarReqDto.getServiceUrl(),
                pulsarReqDto.getId(), pulsarReqDto.getTlsContextDto());
        TenantInfo tenantInfo = pulsarAdmin.tenants().getTenantInfo(tenantName);
        return tenantInfo;
    }

    public void createTenant(PulsarReqDto pulsarReqDto, String tenantName) throws Exception {
        try {
            PulsarAdmin pulsarAdmin = getPulsarAdmin(pulsarReqDto.getServiceUrl(),
                    pulsarReqDto.getId(), pulsarReqDto.getTlsContextDto());
            pulsarAdmin.tenants().createTenant(tenantName, new TenantInfoImpl());
        } catch (Exception e) {
            log.error("creat pulsar tenant fail. ", e);
            throw e;
        }
    }

    public void deleteTenant(PulsarReqDto pulsarReqDto, String tenantName) throws Exception  {
        try {
            PulsarAdmin pulsarAdmin = getPulsarAdmin(pulsarReqDto.getServiceUrl(),
                    pulsarReqDto.getId(), pulsarReqDto.getTlsContextDto());
            pulsarAdmin.tenants().deleteTenant(tenantName);
        } catch (Exception e) {
            log.error("delete pulsar tenant fail. ", e);
            throw e;
        }
    }

    private PulsarAdmin creatPulsarAdmin(String serviceUrl, TlsContextDto tlsContextVo) throws PulsarClientException {
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(serviceUrl);
        if (tlsContextVo.isEnableTls()) {
            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", tlsContextVo.getClientCertFile());
            authParams.put("tlsKeyFile", tlsContextVo.getClientKeyFile());
            Authentication tlsAuth = AuthenticationFactory
                    .create(AuthenticationTls.class.getName(), authParams);

            pulsarAdminBuilder.enableTlsHostnameVerification(false)
                    .allowTlsInsecureConnection(true)
                    .tlsTrustCertsFilePath(tlsContextVo.getCaFile())
                    .authentication(tlsAuth);
        }
        return pulsarAdminBuilder.connectionTimeout(15, TimeUnit.SECONDS).build();
    }

    private PulsarAdmin getPulsarAdmin(String serviceUrl, int id, TlsContextDto tlsContextVo)
            throws PulsarClientException {
        if (!pulsarAdminMap.containsKey(id)) {
            PulsarAdmin pulsarAdmin = creatPulsarAdmin(serviceUrl, tlsContextVo);
            pulsarAdminMap.put(id, pulsarAdmin);
        }
        return pulsarAdminMap.get(id);
    }
}
