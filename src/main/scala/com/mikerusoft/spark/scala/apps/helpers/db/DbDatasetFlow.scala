package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.helpers.db

import infra.spark.DatasetTypes.SparkSessionRowType

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.language.higherKinds

class DbDatasetFlow private (override val execution: SparkSession => Dataset[Row]) extends StartFlow[Row](execution) {
  def this(dbProps: DbProps, query: String) = this(sparkSession => DbDatasetFlow.runQueryViaSpark(dbProps, query, sparkSession))
}

object DbDatasetFlow {

  private[DbDatasetFlow] def runQueryViaSpark(dbProps: DbProps, query: String, sparkSession: SparkSession): Dataset[Row] = {
    val reader = sparkSession.read
    JdbcToOptionReader(reader, dbProps)
      .setWith("driver", props => props.driverPath)
      .setIf("partitionColumn", props => props.partitionColumn)
      .setIf("numPartitions", props => props.numPartitions)
      .setIf("lowerBound", props => props.lowerBound)
      .setIf("upperBound", props => props.upperBound)
      .get.jdbc(dbProps.host, query, dbProps.props())
  }

  def apply(dbProps: DbProps, query: String): SparkSessionRowType = new DbDatasetFlow(dbProps, query) {}


}