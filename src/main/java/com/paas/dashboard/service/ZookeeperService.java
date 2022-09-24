/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.paas.dashboard.service;

import com.paas.dashboard.config.ZooKeeperConfig;
import com.paas.dashboard.module.zookeeper.ZnodeMetric;
import com.paas.dashboard.module.zookeeper.ZooKeeperInstanceMetric;
import com.paas.dashboard.storage.StorageZooKeeper;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class ZookeeperService {

    private final Map<String, ZooKeeperInstanceMetric> zookeeperInstanceMetrics = new ConcurrentHashMap<>();

    public void instanceAnalyze(String id) throws Exception {
        ZooKeeperConfig config = StorageZooKeeper.getInstance().getConfig(id);
        try (
                ZooKeeper zk = new ZooKeeper(config.getZooKeeperAddr(), 15_000, null)
        ) {
            List<String> children = zk.getChildren("/", null);
            List<String> result = new ArrayList<>(children);
            traversal(result, children, zk);
            List<ZnodeMetric> znodeMetrics = new ArrayList<>();
            long sum = 0;
            long maxDataSize = 0;
            for (String znodePath : result) {
                Stat stat = zk.exists(znodePath, null);
                maxDataSize = Math.max(maxDataSize, stat.getDataLength());
                sum += stat.getDataLength();
                znodeMetrics.add(new ZnodeMetric(znodePath, stat.getDataLength()));
            }
            long avg = sum / result.size();
            zookeeperInstanceMetrics.put(id,
                    new ZooKeeperInstanceMetric(result.size(), maxDataSize, avg, znodeMetrics));
        }
    }

    private void traversal(List<String> result, List<String> paths, ZooKeeper zk) throws Exception {
        if (paths.size() == 0) {
            return;
        }
        for (String path : paths) {
            List<String> children = zk.getChildren(path, null);
            result.addAll(children);
            traversal(result, children, zk);
        }
    }

    public Optional<List<ZnodeMetric>> getTopNodes(String id, String type) {
        ZooKeeperInstanceMetric instanceMetric = zookeeperInstanceMetrics.get(id);
        if (instanceMetric == null) {
            return Optional.empty();
        }
        if ("size".equalsIgnoreCase(type) || "".equalsIgnoreCase(type)) {
            List<ZnodeMetric> znodeMetric = instanceMetric.getZnodeMetric();
            znodeMetric.sort((o1, o2) -> (int) (o1.getZnodeDataSize() - o2.getZnodeDataSize()));
            List<ZnodeMetric> znodeMetricList = new ArrayList<>();
            int topN = Math.min(10, znodeMetric.size());
            for (int i = 0; i < topN; i++) {
                znodeMetricList.add(znodeMetric.get(i));
            }
            return Optional.of(znodeMetricList);
        }

        return Optional.of(new ArrayList<>());
    }

}
