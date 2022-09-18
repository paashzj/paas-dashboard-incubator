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

import 'dart:convert';
import 'dart:developer';

import 'package:paas_dashboard_portal_flutter/api/http_util.dart';
import 'package:paas_dashboard_portal_flutter/api/tls_context.dart';
import 'package:paas_dashboard_portal_flutter/module/pulsar/pulsar_tenant.dart';
import 'package:http/http.dart' as http;
import '../../module/pulsar/pulsar_req.dart';
import '../url_const.dart';

class PulsarTenantApi {
  static Future<void> createTenant(int id, String host, int port, TlsContext tlsContext, String tenant) async {
    var url = Uri.parse('${UrlConst.Host}${UrlConst.PulsarCreate}');
    String protocol = tlsContext.enableTls ? HttpUtil.https : HttpUtil.http;
    var response = await http.post(url,
        headers: {"Content-Type": "application/json"},
        body: json.encode(PulsarReq(id, '$protocol$host:$port', tlsContext.toMap())));
    if (HttpUtil.abnormal(response.statusCode)) {
      log('ErrorCode is ${response.statusCode}, body is ${response.body}');
      throw Exception('ErrorCode is ${response.statusCode}, body is ${response.body}');
    }
  }

  static Future<void> deleteTenant(int id, String host, int port, TlsContext tlsContext, String tenant) async {
    var url = Uri.parse('${UrlConst.Host}${UrlConst.PulsarDelete}');
    String protocol = tlsContext.enableTls ? HttpUtil.https : HttpUtil.http;
    var response = await http.post(url,
        headers: {"Content-Type": "application/json"},
        body: json.encode(PulsarReq(id, '$protocol$host:$port', tlsContext.toMap())));
    if (HttpUtil.abnormal(response.statusCode)) {
      log('ErrorCode is ${response.statusCode}, body is ${response.body}');
      throw Exception('ErrorCode is ${response.statusCode}, body is ${response.body}');
    }
  }

  static Future<List<TenantResp>> getTenants(int id, String host, int port, TlsContext tlsContext) async {
    var url = Uri.parse('${UrlConst.Host}${UrlConst.PulsarFetchTenants}');
    String protocol = tlsContext.enableTls ? HttpUtil.https : HttpUtil.http;
    var response = await http.post(url,
        headers: {"Content-Type": "application/json"},
        body: json.encode(PulsarReq(id, '$protocol$host:$port', tlsContext.toMap())));
    if (HttpUtil.abnormal(response.statusCode)) {
      log('ErrorCode is ${response.statusCode}, body is ${response.body}');
      throw Exception('ErrorCode is ${response.statusCode}, body is ${response.body}');
    }
    List jsonResponse = json.decode(response.body) as List;
    return jsonResponse.map((name) => TenantResp.fromJson(name)).toList();
  }

  static Future<String> getTenantInfo(int id, String host, int port, String tenant, TlsContext tlsContext) async {
    var url = Uri.parse('${UrlConst.Host}${UrlConst.PulsarGetTenantInfo}?tenantName=$tenant');
    String protocol = tlsContext.enableTls ? HttpUtil.https : HttpUtil.http;
    var response = await http.post(url,
        headers: {"Content-Type": "application/json"},
        body: json.encode(PulsarReq(id, '$protocol$host:$port', tlsContext.toMap())));
    if (HttpUtil.abnormal(response.statusCode)) {
      log('ErrorCode is ${response.statusCode}, body is ${response.body}');
      throw Exception('ErrorCode is ${response.statusCode}, body is ${response.body}');
    }
    return response.body;
  }
}
