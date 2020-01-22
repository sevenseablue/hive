/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qunar.hive.jdbc.dao;

import com.qunar.hive.jdbc.conf.DatabaseType;
import com.qunar.hive.jdbc.conf.JdbcStorageConfig;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for creating the correct DatabaseAccessor class for the job
 */
public class DatabaseAccessorFactory {

  private DatabaseAccessorFactory() {
  }


  public static DatabaseAccessor getAccessor(DatabaseType dbType) {

    DatabaseAccessor accessor = null;
    switch (dbType) {
      case MYSQL:
        accessor = new MySqlDatabaseAccessor();
        break;
      case MYSQLQMHA:
        accessor = new QmhaJdbcDatabaseAccessor();
        break;
      case MYSQLPXC:
        accessor = new PxcJdbcDatabaseAccessor();
        break;
      case POSTGRES:
        accessor = new PostgresqlDatabaseAccessor();
        break;
      default:
        accessor = new GenericJdbcDatabaseAccessor();
        break;
    }

    return accessor;
  }


  public static DatabaseAccessor getAccessor(Configuration conf) {
    DatabaseType dbType = DatabaseType
        .valueOf(JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
    return getAccessor(dbType);
  }

}
