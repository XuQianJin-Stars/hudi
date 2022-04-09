/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getPartitionPathToTruncate
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

/**
 * Command for truncate hudi table.
 */
case class TruncateHoodieTableCommand(
                                       tableIdentifier: TableIdentifier,
                                       partitionSpec: Option[TablePartitionSpec])
  extends HoodieLeafRunnableCommand with ProvidesHoodieConfig {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fullTableName = s"${tableIdentifier.database}.${tableIdentifier.table}"
    logInfo(s"start execute truncate table command for $fullTableName")

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)

    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableIdentifier)
    val tableIdentWithDB = table.identifier.quotedString

    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableIdentWithDB")
    }

    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
          s"for tables that are not partitioned: $tableIdentWithDB")
    }

    val basePath = hoodieCatalogTable.tableLocation

    val df = sparkSession.sqlContext.read
      .format("hudi")
      .load(basePath)

    if (partitionSpec.isEmpty) {
      df.sqlContext.sql(s"delete from ${hoodieCatalogTable.tableName}")
    } else {
      val resolver = sparkSession.sessionState.conf.resolver
      val partitionsToTruncate: String = getPartitionPathToTruncate(hoodieCatalogTable, table, partitionSpec, resolver)
      df.sqlContext.sql(s"""delete from ${hoodieCatalogTable.tableName} where $partitionsToTruncate""")
    }

    // After deleting the data, refresh the table to make sure we don't keep around a stale
    // file relation in the metastore cache and cached table data in the cache manager.
    sparkSession.catalog.refreshTable(table.identifier.quotedString)
    Seq.empty[Row]
  }
}
