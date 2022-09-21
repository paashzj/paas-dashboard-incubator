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

import com.paas.dashboard.service.PulsarAdminService;
import com.paas.dashboard.storage.StoragePulsar;
import com.paas.dashboard.dto.pulsar.PulsarInstanceDto;
import com.paas.dashboard.dto.pulsar.PulsarTenantCreateReq;

import org.apache.pulsar.common.policies.data.TenantInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.DeleteMapping;

import java.util.List;


@RestController
@RequestMapping("/api/pulsar")
public class PulsarTenantsController {

    private final StoragePulsar storagePulsar = StoragePulsar.getInstance();

    @Autowired
    private PulsarAdminService pulsarAdminService;

    @PostMapping("/instances")
    public HttpStatus saveInstance(@RequestBody PulsarInstanceDto reqSaveInstancesVo) {
        boolean bool = storagePulsar.saveConfig(reqSaveInstancesVo);
        if (bool) {
            return HttpStatus.OK;
        } else {
            return HttpStatus.BAD_REQUEST;
        }
    }

    @GetMapping("/instances/{id}/tenants")
    public List<String> fetchTenants(@PathVariable("id") String id) throws Exception {
        return pulsarAdminService.fetchTenants(id);
    }

    @GetMapping("/instances/{id}/tenants/{tenant}")
    public TenantInfo getTenantInfo(@PathVariable("id") String id,
                                    @PathVariable("tenant") String tenant) throws Exception {
        return pulsarAdminService.getTenantInfo(id, tenant);

    }

    @PutMapping("/instances/{id}/tenants")
    public void createTenant(@PathVariable("id") String id,
                             @RequestBody PulsarTenantCreateReq tenantCreateReq) throws Exception {
        pulsarAdminService.createTenant(id, tenantCreateReq.getTenantName());
    }

    @DeleteMapping("/instances/{id}/tenants/{tenant}")
    public void deleteTenant(@PathVariable("id") String id,
                             @PathVariable("tenant") String tenant) throws Exception {
        pulsarAdminService.deleteTenant(id, tenant);
    }

}
