package com.dy.spark.scala
package apps.helpers.db

import infra.spark.DatasetTypes.SparkSessionRowType
import infra.spark.StartFlow
import infra.spark.db.JdbcToOptionReader

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.language.higherKinds

abstract class DbDatasetFlow private (override val execution: SparkSession => Dataset[Row]) extends StartFlow[Row](execution) {
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