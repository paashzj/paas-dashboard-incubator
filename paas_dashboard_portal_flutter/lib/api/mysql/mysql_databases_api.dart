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

import 'package:paas_dashboard_portal_flutter/module/mysql/mysql_database.dart';
import 'package:paas_dashboard_portal_flutter/module/mysql/mysql_sql_result.dart';
import 'package:paas_dashboard_portal_flutter/module/mysql/mysql_table.dart';
import 'package:paas_dashboard_portal_flutter/module/mysql/mysql_instance_po.dart';
import 'package:paas_dashboard_portal_flutter/ui/component/dynamic_filter_table.dart';

class MysqlDatabaseApi {
  static const String SELECT_ALL = "select * from %s %s limit 100";
  static const String SCHEMA_DB = "information_schema";

  static const String TABLE_COLUMN =
      "select COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE,COLUMN_TYPE  from `COLUMNS` where TABLE_NAME = '%s' and TABLE_SCHEMA = '%s'";

  static const String TABLE_INDEX =
      "select INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, INDEX_TYPE from STATISTICS where TABLE_NAME = '%s' and TABLE_SCHEMA = '%s'";

  static Future<List<DatabaseResp>> getDatabaseList(String host, int port, String username, String password) async {
    throw UnimplementedError();
  }

  static Future<List<TableResp>> getTableList(
      String host, int port, String username, String password, String db) async {
    throw UnimplementedError();
  }

  static Future<MysqlSqlResult> getData(
      MysqlInstancePo mysqlConn, String dbname, String tableName, String where) async {
    throw UnimplementedError();
  }

  static Future<MysqlSqlResult> getSqlData(String sql, MysqlInstancePo mysqlConn, String dbname) async {
    throw UnimplementedError();
  }

  static Future<List<String>> getUsers(String host, int port, String username, String password) async {
    throw UnimplementedError();
  }

  static String getWhere(List<DropDownButtonData>? filters) {
    throw UnimplementedError();
  }

  static String getOPValue(String value, TYPE type, OP op) {
    throw UnimplementedError();
  }

  static String getWhereOP(OP op) {
    throw UnimplementedError();
  }
}
