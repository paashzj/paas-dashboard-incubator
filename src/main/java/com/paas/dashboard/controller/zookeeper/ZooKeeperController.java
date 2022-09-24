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

import com.paas.dashboard.module.zookeeper.ZnodeMetric;
import com.paas.dashboard.service.ZookeeperService;
import jakarta.ws.rs.PathParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/zookeeper")
public class ZooKeeperController {

    @Autowired
    private ZookeeperService zookeeperService;

    @PostMapping("/instance/{id}/analyze")
    public void nodeAnalysis(@PathVariable("id") String id) throws Exception {
        zookeeperService.instanceAnalyze(id);
    }

    @GetMapping("/instance/{id}/statistics/top")
    public ResponseEntity<List<ZnodeMetric>> getTopNodes(@PathVariable("id") String id,
                                      @PathParam("type") String type) throws Exception {
        Optional<List<ZnodeMetric>> znodeMetricListOp = zookeeperService.getTopNodes(id, type);
        if (znodeMetricListOp.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(znodeMetricListOp.get(), HttpStatus.OK);
    }

}
