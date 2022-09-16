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

import 'package:paas_dashboard_portal_flutter/api/url_const.dart';
import 'package:paas_dashboard_portal_flutter/module/pulsar/const.dart';
import 'package:paas_dashboard_portal_flutter/module/pulsar/pulsar_instance_po.dart';
import 'package:http/http.dart' as http;
import 'dart:async';
import 'dart:convert';

class PulsarInstanceApi {
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
    var url = Uri.parse('${UrlConst.Host}${UrlConst.PulsarInstance}');
    var pulsarInstancePo = PulsarInstanceDto(1, name, host, port, functionHost, functionPort, enableTls,
        functionEnableTls, caFile, clientCertFile, clientKeyFile, clientKeyPassword);
    var body = json.encode(pulsarInstancePo.toMap());
    var res = await http.post(url, headers: {"Content-Type": "application/json"}, body: body);
    print("pulsar save ret : ${res.body}");
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

  static Future<List<PulsarInstanceDto>> pulsarInstances() async {
    return [
      PulsarInstanceDto(
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

  static Future<PulsarInstanceDto?> pulsarInstance(String name) async {
    throw UnimplementedError();
  }
}
