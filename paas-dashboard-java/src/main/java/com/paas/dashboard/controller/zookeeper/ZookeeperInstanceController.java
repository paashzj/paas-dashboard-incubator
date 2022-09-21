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

package com.paas.dashboard.controller.zookeeper;

import com.paas.dashboard.config.ZooKeeperConfig;
import com.paas.dashboard.module.zookeeper.ZooKeeperInstanceCreateReq;
import com.paas.dashboard.module.zookeeper.ZooKeeperInstanceCreateResp;
import com.paas.dashboard.storage.StorageZooKeeper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/zookeeper")
public class ZookeeperInstanceController {

    private final StorageZooKeeper storage = StorageZooKeeper.getInstance();

    @PutMapping("/instances")
    public ResponseEntity<ZooKeeperInstanceCreateResp> save(@RequestBody ZooKeeperInstanceCreateReq req) {
        ZooKeeperConfig zooKeeperConfig = ZooKeeperConfig.genFromReq(req);
        boolean result = storage.saveConfig(zooKeeperConfig);
        if (!result) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(new ZooKeeperInstanceCreateResp(zooKeeperConfig.getId()), HttpStatus.CREATED);
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
    public ResponseEntity<List<ZooKeeperConfig>> listInstances() {
        return new ResponseEntity<>(storage.getConfigMap().values().stream().toList(), HttpStatus.OK);
    }

}
