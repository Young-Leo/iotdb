/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;

abstract class AbstractAlterOrDropTableTask implements IConfigTask {

  protected final String database;

  protected final String tableName;

  protected final String queryId;

  protected final boolean tableIfExists;

  protected AbstractAlterOrDropTableTask(
      final String database,
      final String tableName,
      final String queryId,
      final boolean tableIfExists) {
    this.database = PathUtils.qualifyDatabaseName(database);
    this.tableName = tableName;
    this.queryId = queryId;
    this.tableIfExists = tableIfExists;
  }
}
