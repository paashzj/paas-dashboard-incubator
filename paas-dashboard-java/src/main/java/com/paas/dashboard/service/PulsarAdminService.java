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

import com.paas.dashboard.module.pulsar.PulsarUpdateBacklogQuotaReq;
import com.paas.dashboard.module.pulsar.PulsarAutoTopicCreationReq;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;

@Service
@Slf4j
public class PulsarAdminService {

    private final Map<String, PulsarAdmin> pulsarAdminMap = new HashMap<>();

    public List<String> fetchTenants(String id) throws Exception {
        return null;
    }

    public TenantInfo getTenantInfo(String id, String tenantName) throws Exception {
        return null;
    }

    public void createTenant(String id, String tenantName) throws Exception {

    }

    public void deleteTenant(String id, String tenant) throws Exception {

    }

    private PulsarAdmin creatPulsarAdmin(String id) throws PulsarClientException {
        return null;
    }

    private PulsarAdmin getPulsarAdmin(String id) throws PulsarClientException {
        if (!pulsarAdminMap.containsKey(id)) {
            PulsarAdmin pulsarAdmin = creatPulsarAdmin(id);
            pulsarAdminMap.put(id, pulsarAdmin);
        }
        return pulsarAdminMap.get(id);
    }

    public void createNamespace(String id, String tenantName, String namespace) throws Exception {
    }

    public void deleteNamespace(String id, String tenantName, String namespace) throws Exception {
    }

    public List<String> getNamespaces(String id, String tenantName) throws Exception {
        return null;
    }

    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuota(String id,
                                                                            String tenantName, String namespace)
            throws Exception {
        return null;
    }

    public Policies getPolicy(String id, String tenantName, String namespace) throws Exception {
        return null;
    }

    public void setMessageTTLSecond(String id, String tenantName,
                                    String namespace, int messageTTLSecond) throws Exception {
    }

    public void setMaxProducersPerTopic(String id, String tenantName,
                                        String namespace, int maxProducersPerTopic) throws Exception {
    }

    public void setMaxConsumersPerTopic(String id, String tenantName,
                                        String namespace, int maxConsumersPerTopic) throws Exception {
    }

    public void setMaxConsumerPerSubscription(String id, String tenantName,
                                              String namespace, int maxConsumersPerSubscription) throws Exception {
    }

    public void setMaxUnackedMessagesPerConsumer(String id, String tenantName, String namespace,
                                                 int maxUnackedMessagesPerConsumer) throws Exception {
    }

    public void setMaxUnackedMessagesPerSubscription(String id, String tenantName, String namespace,
                                                     int maxUnackedMessagesPerSubscription) throws Exception {
    }

    public void setMaxSubscriptionsPerTopic(String id, String tenantName,
                                            String namespace, int maxSubscriptionsPerTopic) throws Exception {
    }

    public void setMaxTopicsPerNamespace(String id, String tenantName,
                                         String namespace, int maxTopicsPerNamespace) throws Exception {
    }

    public void updateBacklogQuota(PulsarUpdateBacklogQuotaReq pulsarUpdateBacklogQuotaReq,
                                   String tenantName, String namespace, String id) throws Exception {
    }

    public void setAutoTopicCreation(String id, String tenantName,
                                     String namespace, PulsarAutoTopicCreationReq pulsarReq) throws Exception {
    }

}
