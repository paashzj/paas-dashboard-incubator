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

package com.paas.dashboard.controller.pulsar;

import com.paas.dashboard.config.PulsarConfig;
import com.paas.dashboard.module.pulsar.PulsarInstanceCreateReq;
import com.paas.dashboard.module.pulsar.PulsarInstanceCreateResp;
import com.paas.dashboard.service.PulsarAdminService;
import com.paas.dashboard.storage.StoragePulsar;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class PulsarInstanceController {

    private final StoragePulsar storage = StoragePulsar.getInstance();

    @PutMapping("/instances")
    public ResponseEntity<PulsarInstanceCreateResp> save(@RequestBody PulsarInstanceCreateReq req) {
        PulsarConfig config = PulsarConfig.genFromReq(req);
        boolean result = storage.saveConfig(config);
        if (!result) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(new PulsarInstanceCreateResp(config.getId()), HttpStatus.CREATED);
    }

    @DeleteMapping("/instances/{id}")
    public ResponseEntity<Void> delete(@PathVariable("id") String id) {
        boolean result = storage.deleteConfig(id);
        if (result) {
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/instances")
    public ResponseEntity<List<PulsarConfig>> listInstances() {
        return new ResponseEntity<>(storage.getConfigMap().values().stream().toList(), HttpStatus.OK);
    }

    @Autowired
    private PulsarAdminService pulsarAdminService;
    @PutMapping("/instances/{id}/save-all-topics")
    public ResponseEntity<Void> saveAllTopics(@PathVariable("id") String id) throws Exception {
        pulsarAdminService.saveAllTopics(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/instances/{id}/recover-topics")
    public ResponseEntity<Void> recoverTopics(@PathVariable("id") String id) throws Exception {
        pulsarAdminService.recoverTopics(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
