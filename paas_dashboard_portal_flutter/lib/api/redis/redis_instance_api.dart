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

import 'package:paas_dashboard_portal_flutter/module/redis/const.dart';
import 'package:paas_dashboard_portal_flutter/module/redis/redis_instance_po.dart';

class RedisInstanceApi {
  static Future<void> saveRedis(String name, String addr, String username, String password) async {
    throw UnimplementedError();
  }

  static Future<void> deleteRedis(int id) async {
    throw UnimplementedError();
  }

  static Future<List<RedisInstanceDto>> redisInstances() async {
    return [RedisInstanceDto(0, "example", RedisConst.defaultIp, RedisConst.defaultPort, RedisConst.defaultPassword)];
  }

  static Future<RedisInstanceDto?> redisInstance(String name) async {
    throw UnimplementedError();
  }
}
