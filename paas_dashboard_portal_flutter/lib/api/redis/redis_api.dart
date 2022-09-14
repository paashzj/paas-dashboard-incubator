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

class RedisApi {
  static Future<dynamic> set(String host, int port, String password, String key, String value) async {
    throw UnimplementedError();
  }

  static Future<dynamic> get(String host, int port, String password, String key) async {
    throw UnimplementedError();
  }

  static Future<dynamic> delete(String host, int port, String password, String key) async {
    throw UnimplementedError();
  }

  static dynamic keys(String host, int port, String password, String patten) async {
    throw UnimplementedError();
  }

  static Future<dynamic> hGet(String host, int port, String password, String key, String field) async {
    throw UnimplementedError();
  }

  static Future<dynamic> hSet(
      String host, int port, String password, String key, String field, String fieldValue) async {
    throw UnimplementedError();
  }

  static Future<dynamic> hGetAll(String host, int port, String password, String key) async {
    throw UnimplementedError();
  }

  static Future<dynamic> hDel(String host, int port, String password, String key, String field) async {
    throw UnimplementedError();
  }
}
