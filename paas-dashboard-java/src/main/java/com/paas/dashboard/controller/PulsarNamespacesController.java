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

package com.paas.dashboard.controller;

import com.paas.dashboard.dto.pulsar.PulsarUpdateBacklogQuotaReq;
import com.paas.dashboard.dto.pulsar.PulsarMessageTTLSecondReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxProducersPerTopicReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxConsumersPerSubscriptionReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxConsumersPerTopicReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxUnackedMessagesPerConsumerReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxUnackedMessagesPerSubscriptionReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxSubscriptionsPerTopicReq;
import com.paas.dashboard.dto.pulsar.PulsarMaxTopicsPerNamespaceReq;
import com.paas.dashboard.dto.pulsar.PulsarAutoTopicCreationReq;
import com.paas.dashboard.service.PulsarAdminService;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/pulsar/instances/{id}/tenants/{tenant}")
public class PulsarNamespacesController {

    @Autowired
    private PulsarAdminService pulsarAdminService;

    @PostMapping("/namespaces/{namespace}")
    public void createNamespace(@PathVariable("id") String id,
                                @PathVariable("tenant") String tenant,
                                @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.createNamespace(id, tenant, namespace);
    }

    @GetMapping("/namespaces")
    public List<String> getNamespaces(@PathVariable("id") String id,
                                      @PathVariable("tenant") String tenant) throws Exception {
        return pulsarAdminService.getNamespaces(id, tenant);
    }

    @DeleteMapping("/namespaces/{namespace}")
    public void deleteNamespace(@PathVariable("id") String id,
                                @PathVariable("tenant") String tenant,
                                @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.deleteNamespace(id, tenant, namespace);
    }

    @GetMapping("/namespaces/{namespace}/get-backlog-quota")
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuota(@PathVariable("id") String id,
                                                                            @PathVariable("tenant") String tenant,
                                                                            @PathVariable("namespace") String namespace)
            throws Exception {
        return pulsarAdminService.getBacklogQuota(id, tenant, namespace);
    }


    @PutMapping("/namespaces/{namespace}/properties/update-backlog-quota")
    public void updateBacklogQuota(@RequestBody PulsarUpdateBacklogQuotaReq pulsarUpdateBacklogQuotaReq,
                                   @PathVariable("id") String id,
                                   @PathVariable("tenant") String tenant,
                                   @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.updateBacklogQuota(pulsarUpdateBacklogQuotaReq, id, tenant, namespace);
    }

    @GetMapping("/namespaces/{namespace}/get-policy")
    public Policies getPolicy(@PathVariable("id") String id,
                              @PathVariable("tenant") String tenant,
                              @PathVariable("namespace") String namespace) throws Exception {
        return pulsarAdminService.getPolicy(id, tenant, namespace);
    }

    @PutMapping("/namespaces/{namespace}/properties/set-message-ttl-second")
    public void setMessageTTLSecond(@PathVariable("id") String id,
                                    @PathVariable("tenant") String tenant,
                                    @PathVariable("namespace") String namespace,
                                    @RequestBody PulsarMessageTTLSecondReq messageTTLSecondReq) throws Exception {

        pulsarAdminService.setMessageTTLSecond(id, tenant, namespace, messageTTLSecondReq.getMessageTTLSecond());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-producers-per-topic")
    public void setMaxProducersPerTopic(@RequestBody PulsarMaxProducersPerTopicReq maxProducersPerTopicReq,
                                        @PathVariable("id") String id,
                                        @PathVariable("tenant") String tenant,
                                        @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxProducersPerTopic(id, tenant, namespace,
                maxProducersPerTopicReq.getMaxProducersPerTopic());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-consumers-per-topic")
    public void serMaxConsumersPerTopic(@RequestBody PulsarMaxConsumersPerTopicReq maxConsumersPerTopicReq,
                                        @PathVariable("id") String id,
                                        @PathVariable("tenant") String tenant,
                                        @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxConsumersPerTopic(id, tenant, namespace,
                maxConsumersPerTopicReq.getMaxConsumersPerTopic());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-consumers-per-subscription")
    public void setMaxConsumerPerSubscription(@RequestBody PulsarMaxConsumersPerSubscriptionReq pulsarReqDto,
                                              @PathVariable("id") String id,
                                              @PathVariable("tenant") String tenant,
                                              @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxConsumerPerSubscription(id, tenant, namespace,
                pulsarReqDto.getMaxConsumersPerSubscription());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-unacked-messages-per-consumer")
    public void setMaxUnackedMessagesPerConsumer(@RequestBody PulsarMaxUnackedMessagesPerConsumerReq pulsarReqDto,
                                                 @PathVariable("id") String id,
                                                 @PathVariable("tenant") String tenant,
                                                 @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxUnackedMessagesPerConsumer(id, tenant, namespace,
                pulsarReqDto.getMaxUnackedMessagesPerConsumer());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-unacked-messages-per-subscription")
    public void setMaxUnackedMessagesPerSubscription(@RequestBody
                                                     PulsarMaxUnackedMessagesPerSubscriptionReq pulsarReqDto,
                                                     @PathVariable("id") String id,
                                                     @PathVariable("tenant") String tenant,
                                                     @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxUnackedMessagesPerSubscription(id, tenant, namespace,
                pulsarReqDto.getMaxUnackedMessagesPerSubscription());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-subscriptions-per-topic")
    public void setMaxSubscriptionsPerTopic(@RequestBody PulsarMaxSubscriptionsPerTopicReq pulsarReqDto,
                                            @PathVariable("id") String id,
                                            @PathVariable("tenant") String tenant,
                                            @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxSubscriptionsPerTopic(id, tenant, namespace,
                pulsarReqDto.getMaxSubscriptionsPerTopic());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-max-topics-per-namespace")
    public void setMaxTopicsPerNamespace(@RequestBody PulsarMaxTopicsPerNamespaceReq pulsarReqDto,
                                         @PathVariable("id") String id,
                                         @PathVariable("tenant") String tenant,
                                         @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setMaxTopicsPerNamespace(id, tenant, namespace,
                pulsarReqDto.getMaxTopicsPerNamespace());
    }

    @PutMapping("/namespaces/{namespace}/properties/set-auto-topic-creation")
    public void setAutoTopicCreation(@RequestBody PulsarAutoTopicCreationReq pulsarReqDto,
                                     @PathVariable("id") String id,
                                     @PathVariable("tenant") String tenant,
                                     @PathVariable("namespace") String namespace) throws Exception {
        pulsarAdminService.setAutoTopicCreation(id, tenant, namespace, pulsarReqDto);
    }
}
