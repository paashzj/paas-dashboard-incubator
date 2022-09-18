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

import com.paas.dashboard.dto.PulsarReqDto;
import com.paas.dashboard.service.PulsarAdminService;
import com.paas.dashboard.storage.StoragePulsar;
import com.paas.dashboard.dto.ReqSaveInstancesDto;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.PathParam;
import java.util.List;


@RestController
@RequestMapping("/api/pulsar")
public class PulsarController {

    private final StoragePulsar storagePulsar = StoragePulsar.getInstance();

    @Autowired
    private PulsarAdminService pulsarAdminService;

    @PostMapping("/instances")
    public HttpStatus saveInstance(@RequestBody ReqSaveInstancesDto reqSaveInstancesVo) {
        boolean bool = storagePulsar.saveConfig(reqSaveInstancesVo);
        if (bool) {
            return HttpStatus.OK;
        } else {
            return HttpStatus.BAD_REQUEST;
        }
    }

    @PostMapping("/tenants/fetchTenants")
    public List<String> fetchTenants(@RequestBody PulsarReqDto pulsarReqDto) throws Exception {
        return pulsarAdminService.fetchTenants(pulsarReqDto);
    }

    @PostMapping("/tenants/getTenantInfo")
    public TenantInfo getTenantInfo(@RequestBody PulsarReqDto pulsarReqDto,
                                    @PathParam("tenantName") String tenantName) throws Exception {
        return pulsarAdminService.getTenantInfo(pulsarReqDto, tenantName);

    }

    @PostMapping("/tenants/create")
    public void createTenant(@RequestBody PulsarReqDto pulsarReqDto,
                             @PathParam("tenantName") String tenantName) throws Exception {
        pulsarAdminService.createTenant(pulsarReqDto, tenantName);
    }

    @PostMapping("/tenants/delete")
    public void deleteTenant(@RequestBody PulsarReqDto pulsarReqDto,
                                   @PathParam("tenantName") String tenantName) throws Exception {
        pulsarAdminService.deleteTenant(pulsarReqDto, tenantName);
    }

}
