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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.paas.dashboard.config.PulsarConfig;
import com.paas.dashboard.module.pulsar.PulsarStorageInfo;
import com.paas.dashboard.module.pulsar.PulsarUpdateBacklogQuotaReq;
import com.paas.dashboard.module.pulsar.PulsarAutoTopicCreationReq;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.paas.dashboard.storage.StoragePulsar;
import com.paas.dashboard.storage.StorageTopics;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class PulsarAdminService {

    private AsyncLoadingCache<String, PulsarAdmin> pulsarAdminCache;

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

    private CompletableFuture<PulsarAdmin> creatPulsarAdmin(String id) throws PulsarClientException {
        StoragePulsar storage = StoragePulsar.getInstance();
        PulsarConfig config = storage.getConfig(id);
        String serviceHttpUrl = String.format(config.isEnableTls() ? "https://%s:%d" : "http://%s:%d",
                config.getHost(), config.getPort());
        CompletableFuture<PulsarAdmin> future = new CompletableFuture<>();
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
        future.complete(pulsarAdminBuilder.connectionTimeout(15, TimeUnit.SECONDS).build());
        return future;
    }


    public void saveAllTopics(String id) throws Exception {
        PulsarAdmin pulsarAdmin = getPulsarAdmin(id);
        List<String> tenants = pulsarAdmin.tenants().getTenants();
        PulsarStorageInfo storageInfo = new PulsarStorageInfo();
        Map<String, TenantInfo> tenantInfoMap =  new HashMap<>();
        Map<String, Policies> namespaceInfoMap =  new HashMap<>();
        Map<String, PartitionedTopicMetadata> topicsMap =  new HashMap<>();
        for (String tenant : tenants) {
            tenantInfoMap.put(tenant, pulsarAdmin.tenants().getTenantInfo(tenant));
            List<String> namespaces = pulsarAdmin.namespaces().getNamespaces(tenant);
            for (String namespace : namespaces) {
                namespaceInfoMap.put(namespace, pulsarAdmin.namespaces().getPolicies(namespace));
                List<String> topics = pulsarAdmin.topics().getList(namespace);
                for (String topic : topics) {
                    topicsMap.put(topic, pulsarAdmin.topics().getPartitionedTopicMetadataAsync(topic).get());
                }
            }
        }
        storageInfo.setTenants(tenantInfoMap);
        storageInfo.setNamespaces(namespaceInfoMap);
        storageInfo.setTopics(topicsMap);
        StorageTopics.getInstance().saveConfig(storageInfo);
    }

    public void recoverTopics(String id) throws Exception {
        PulsarAdmin pulsarAdmin = getPulsarAdmin(id);
        PulsarStorageInfo storageInfo = StorageTopics.getInstance().getConfig(id);
        for (Map.Entry<String, TenantInfo> tenantEntry : storageInfo.getTenants().entrySet()) {
            pulsarAdmin.tenants().createTenant(tenantEntry.getKey(), tenantEntry.getValue());
        }
        for (Map.Entry<String, Policies> namespaceEntry : storageInfo.getNamespaces().entrySet()) {
            pulsarAdmin.namespaces().createNamespace(namespaceEntry.getKey(), namespaceEntry.getValue());
        }
        for (Map.Entry<String, PartitionedTopicMetadata> topicEntry : storageInfo.getTopics().entrySet()) {
            pulsarAdmin.topics().createPartitionedTopic(topicEntry.getKey(),
                    topicEntry.getValue().partitions, topicEntry.getValue().properties);
        }
        StorageTopics.getInstance().deleteConfig(id);
    }

    private PulsarAdmin getPulsarAdmin(String id) throws ExecutionException, InterruptedException {
        return pulsarAdminCache.get(id).get();
    }

    @PostConstruct
    public void init() {
        pulsarAdminCache = Caffeine.newBuilder()
                .expireAfterAccess(60 * 60 * 24, TimeUnit.SECONDS)
                .maximumSize(100)
                .removalListener((RemovalListener<String, PulsarAdmin>) (id, pulsarAdmin, cause) -> {
                    log.info("id {} cache removed, because of {}", id, cause);
                    try {
                        pulsarAdmin.close();
                    } catch (Exception e) {
                        log.error("close failed, ", e);
                    }
                }).buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public CompletableFuture<? extends PulsarAdmin> asyncLoad(String id, Executor executor)
                            throws Exception {
                        return creatPulsarAdmin(id);
                    }

                    @Override
                    public CompletableFuture<? extends PulsarAdmin> asyncReload(String id, PulsarAdmin oldPulsarAdmin,
                                                                                Executor executor) throws Exception {
                        return creatPulsarAdmin(id);
                    }
                });
    }
}
