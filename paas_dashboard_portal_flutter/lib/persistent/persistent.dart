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

import 'package:paas_dashboard_portal_flutter/module/bk/const.dart';
import 'package:paas_dashboard_portal_flutter/module/mongo/const.dart';
import 'package:paas_dashboard_portal_flutter/module/mysql/const.dart';
import 'package:paas_dashboard_portal_flutter/module/pulsar/const.dart';
import 'package:paas_dashboard_portal_flutter/module/redis/const.dart';
import 'package:paas_dashboard_portal_flutter/module/ssh/ssh_step.dart';
import 'package:paas_dashboard_portal_flutter/module/zk/const.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/bk_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/k8s_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/mongo_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/mysql_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/pulsar_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/redis_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/persistent/po/zk_instance_po.dart';

class PulsarFormDto {
  late int id;
  late String name = "";
  late String host = "";
  late int port = 8080;
  late String functionHost = "";
  late int functionPort = 6650;
  late bool enableTls = false;
  late bool functionEnableTls = false;
  late String caFile = "";
  late String clientCertFile = "";
  late String clientKeyFile = "";
  late String clientKeyPassword = "";

  PulsarFormDto();
}

class Persistent {
  static Future<void> savePulsar(
      String name,
      String host,
      int port,
      String functionHost,
      int functionPort,
      bool enableTls,
      bool functionEnableTls,
      String caFile,
      String clientCertFile,
      String clientKeyFile,
      String clientKeyPassword) async {
    throw UnimplementedError();
  }

  static Future<void> updatePulsar(
      int id,
      String name,
      String host,
      int port,
      String functionHost,
      int functionPort,
      bool enableTls,
      bool functionEnableTls,
      String caFile,
      String clientCertFile,
      String clientKeyFile,
      String clientKeyPassword) async {
    throw UnimplementedError();
  }

  static Future<void> deletePulsar(int id) async {
    throw UnimplementedError();
  }

  static Future<List<PulsarInstancePo>> pulsarInstances() async {
    return [
      PulsarInstancePo(
          0,
          "example",
          PulsarConst.defaultHost,
          PulsarConst.defaultBrokerPort,
          PulsarConst.defaultHost,
          PulsarConst.defaultFunctionPort,
          PulsarConst.defaultEnableTls == 1,
          PulsarConst.defaultFunctionEnableTls == 1,
          PulsarConst.defaultCaFile,
          PulsarConst.defaultClientCertFile,
          PulsarConst.defaultClientKeyFile,
          PulsarConst.defaultClientKeyPassword)
    ];
  }

  static Future<PulsarInstancePo?> pulsarInstance(String name) async {
    throw UnimplementedError();
  }

  static Future<void> saveBookkeeper(String name, String host, int port) async {
    throw UnimplementedError();
  }

  static Future<void> deleteBookkeeper(int id) async {
    throw UnimplementedError();
  }

  static Future<List<BkInstancePo>> bookkeeperInstances() async {
    return [BkInstancePo(0, "example", BkConst.defaultHost, BkConst.defaultPort)];
  }

  static Future<BkInstancePo?> bookkeeperInstance(String name) async {
    throw UnimplementedError();
  }

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

  static Future<void> saveKubernetesSsh(String name, List<SshStep> sshSteps) async {
    throw UnimplementedError();
  }

  static Future<void> deleteKubernetes(int id) async {
    throw UnimplementedError();
  }

  static Future<List<K8sInstancePo>> kubernetesInstances() async {
    return [K8sInstancePo(0, "example")];
  }

  static Future<void> saveMongo(String name, String addr, String username, String password) async {
    throw UnimplementedError();
  }

  static Future<void> deleteMongo(int id) async {
    throw UnimplementedError();
  }

  static Future<List<MongoInstancePo>> mongoInstances() async {
    return [MongoInstancePo(0, "example", MongoConst.defaultAddr, "", "")];
  }

  static Future<void> saveMysql(String name, String host, int port, String username, String password) async {
    throw UnimplementedError();
  }

  static Future<void> deleteMysql(int id) async {
    throw UnimplementedError();
  }

  static Future<List<MysqlInstancePo>> mysqlInstances() async {
    return [
      MysqlInstancePo(0, "example", MysqlConst.defaultHost, MysqlConst.defaultPort, MysqlConst.defaultUsername,
          MysqlConst.defaultPassword)
    ];
  }

  static Future<void> saveRedis(String name, String addr, String username, String password) async {
    throw UnimplementedError();
  }

  static Future<void> deleteRedis(int id) async {
    throw UnimplementedError();
  }

  static Future<List<RedisInstancePo>> redisInstances() async {
    return [RedisInstancePo(0, "example", RedisConst.defaultIp, RedisConst.defaultPort, RedisConst.defaultPassword)];
  }

  static Future<RedisInstancePo?> redisInstance(String name) async {
    throw UnimplementedError();
  }
}
