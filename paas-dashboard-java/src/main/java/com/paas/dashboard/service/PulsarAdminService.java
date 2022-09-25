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

import com.paas.dashboard.config.PulsarConfig;
import com.paas.dashboard.module.pulsar.PulsarClientInfo;
import com.paas.dashboard.module.pulsar.PulsarUpdateBacklogQuotaReq;
import com.paas.dashboard.module.pulsar.PulsarAutoTopicCreationReq;

import com.paas.dashboard.storage.StoragePulsar;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@EnableScheduling
public class PulsarAdminService {

    private final Map<String, PulsarClientInfo> pulsarAdminMap = new HashMap<>();

    public List<String> fetchTenants(String id) throws Exception {
        return getPulsarAdmin(id).tenants().getTenants();
    }

    public TenantInfo getTenantInfo(String id, String tenant) throws Exception {
        return getPulsarAdmin(id).tenants().getTenantInfo(tenant);
    }

    public void createTenant(String id, String tenant) throws Exception {
        getPulsarAdmin(id).tenants().createTenant(tenant, TenantInfo.builder().build());
    }

    public void deleteTenant(String id, String tenant) throws Exception {
        getPulsarAdmin(id).tenants().deleteTenant(tenant);
    }

    public void createNamespace(String id, String tenant, String namespace) throws Exception {
        getPulsarAdmin(id).namespaces().createNamespace(String.format("%s/%s", tenant, namespace));
    }

    public void deleteNamespace(String id, String tenant, String namespace) throws Exception {
        getPulsarAdmin(id).namespaces().deleteNamespace(String.format("%s/%s", tenant, namespace));
    }

    public List<String> getNamespaces(String id, String tenant) throws Exception {
        return getPulsarAdmin(id).namespaces().getNamespaces(tenant);
    }

    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuota(String id, String tenant,
                                                                            String namespace) throws Exception {
        return getPulsarAdmin(id).namespaces().getBacklogQuotaMap(String.format("%s/%s", tenant, namespace));
    }

    public Policies getPolicy(String id, String tenant, String namespace) throws Exception {
        return getPulsarAdmin(id).namespaces().getPolicies(String.format("%s/%s", tenant, namespace));
    }

    public void setMessageTTLSecond(String id, String tenant, String namespace, int messageTTLSecond) throws Exception {
        getPulsarAdmin(id).namespaces()
                .setNamespaceMessageTTL(String.format("%s/%s", tenant, namespace), messageTTLSecond);
    }

    public void setMaxProducersPerTopic(String id, String tenant,
                                        String namespace, int maxProducersPerTopic) throws Exception {
        getPulsarAdmin(id).namespaces()
                .setMaxProducersPerTopic(String.format("%s/%s", tenant, namespace), maxProducersPerTopic);
    }

    public void setMaxConsumersPerTopic(String id, String tenant,
                                        String namespace, int maxConsumersPerTopic) throws Exception {
        getPulsarAdmin(id).namespaces()
                .setMaxConsumersPerTopic(String.format("%s/%s", tenant, namespace), maxConsumersPerTopic);
    }

    public void setMaxConsumerPerSubscription(String id, String tenant,
                                              String namespace, int maxConsumersPerSubscription) throws Exception {
        getPulsarAdmin(id).namespaces()
                .setMaxConsumersPerSubscription(String.format("%s/%s", tenant, namespace), maxConsumersPerSubscription);
    }

    public void setMaxUnackedMessagesPerConsumer(String id, String tenant, String namespace,
                                                 int maxUnackedMessagesPerConsumer) throws Exception {
        String namespaces = String.format("%s/%s", tenant, namespace);
        getPulsarAdmin(id).namespaces()
                .setMaxUnackedMessagesPerConsumer(namespaces, maxUnackedMessagesPerConsumer);
    }

    public void setMaxUnackedMessagesPerSubscription(String id, String tenant, String namespace,
                                                     int maxUnackedMessagesPerSubscription) throws Exception {
        String namespaces = String.format("%s/%s", tenant, namespace);
        getPulsarAdmin(id).namespaces()
                .setMaxUnackedMessagesPerSubscription(namespaces, maxUnackedMessagesPerSubscription);
    }

    public void setMaxSubscriptionsPerTopic(String id, String tenant,
                                            String namespace, int maxSubscriptionsPerTopic) throws Exception {
        getPulsarAdmin(id).namespaces()
                .setMaxSubscriptionsPerTopic(String.format("%s/%s", tenant, namespace), maxSubscriptionsPerTopic);
    }

    public void setMaxTopicsPerNamespace(String id, String tenant,
                                         String namespace, int maxTopicsPerNamespace) throws Exception {
        getPulsarAdmin(id).namespaces()
                .setMaxTopicsPerNamespace(String.format("%s/%s", tenant, namespace), maxTopicsPerNamespace);
    }

    public void updateBacklogQuota(PulsarUpdateBacklogQuotaReq pulsarUpdateBacklogQuotaReq,
                                   String tenant, String namespace, String id) throws Exception {
        BacklogQuota backlogQuota = BacklogQuotaImpl.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.valueOf(pulsarUpdateBacklogQuotaReq.getPolicy()))
                .limitSize(pulsarUpdateBacklogQuotaReq.getLimit())
                .limitTime(pulsarUpdateBacklogQuotaReq.getLimitTime()).build();

        getPulsarAdmin(id).namespaces().setBacklogQuota(String.format("%s/%s", tenant, namespace), backlogQuota);
    }

    public void setAutoTopicCreation(String id, String tenant,
                                     String namespace, PulsarAutoTopicCreationReq pulsarReq) throws Exception {
        AutoTopicCreationOverrideImpl autoTopicCreationOverride = AutoTopicCreationOverrideImpl.builder()
                .defaultNumPartitions(pulsarReq.getDefaultNumPartitions())
                .topicType(pulsarReq.getTopicType())
                .build();
        getPulsarAdmin(id).namespaces()
                .setAutoTopicCreation(String.format("%s/%s", tenant, namespace), autoTopicCreationOverride);
    }

    private PulsarAdmin creatPulsarAdmin(String id) throws PulsarClientException {
        StoragePulsar storage = StoragePulsar.getInstance();
        PulsarConfig config = storage.getConfig(id);
        String serviceHttpUrl = String.format(config.isEnableTls() ? "https://%s:%d" : "http://%s:%d",
                config.getHost(), config.getPort());
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl);
        if (config.isEnableTls()) {
            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", config.getClientCertFile());
            authParams.put("tlsKeyFile", config.getClientKeyFile());
            Authentication tlsAuth = AuthenticationFactory
                    .create(AuthenticationTls.class.getName(), authParams);

            pulsarAdminBuilder.enableTlsHostnameVerification(false)
                    .allowTlsInsecureConnection(true)
                    .tlsTrustCertsFilePath(config.getCaFile())
                    .authentication(tlsAuth);
        }
        return pulsarAdminBuilder.connectionTimeout(15, TimeUnit.SECONDS).build();
    }

    private PulsarAdmin getPulsarAdmin(String id) throws PulsarClientException {
        if (!pulsarAdminMap.containsKey(id)) {
            PulsarAdmin pulsarAdmin = creatPulsarAdmin(id);
            PulsarClientInfo pulsarClientInfo = new PulsarClientInfo(System.currentTimeMillis(), pulsarAdmin);
            pulsarAdminMap.put(id, pulsarClientInfo);
        }
        return pulsarAdminMap.get(id).getPulsarAdmin();
    }

    @Scheduled(cron = "* 3 * * * ?")
    public void cleanOutDateInstances() {
        if (pulsarAdminMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, PulsarClientInfo> entry : pulsarAdminMap.entrySet()) {
            PulsarClientInfo pulsarClientInfo = entry.getValue();
            if (System.currentTimeMillis() - pulsarClientInfo.getCreateTime() >= 3 * 60_1000) {
                pulsarAdminMap.remove(entry.getKey());
            }
        }
    }
}
