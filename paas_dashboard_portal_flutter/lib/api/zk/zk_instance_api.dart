//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

import 'package:paas_dashboard_portal_flutter/module/zk/const.dart';
import 'package:paas_dashboard_portal_flutter/module/zk/zk_instance_po.dart';

class ZkInstanceApi {
  static Future<void> saveZooKeeper(String name, String host, int port) async {
    throw UnimplementedError();
  }

  static Future<void> deleteZooKeeper(int id) async {
    throw UnimplementedError();
  }

  static Future<List<ZkInstancePo>> zooKeeperInstances() async {
    return [ZkInstancePo(0, "example", ZkConst.defaultHost, ZkConst.defaultPort)];
  }

  static Future<ZkInstancePo?> zooKeeperInstance(String name) async {
    throw UnimplementedError();
  }
}
