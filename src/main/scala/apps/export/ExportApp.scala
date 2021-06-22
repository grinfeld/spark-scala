package com.dy.spark.scala
package apps.`export`

import apps.ExecutedApp
import apps.`export`.db.VarEngDbFlow
import apps.helpers.db.DbProps
import infra.FlowOutput
import infra.spark.DatasetTypes.SparkSessionType
import infra.spark.{FlowFromDatasetPairFlow, FromDatasetPairFlow}

import org.apache.spark.sql.{Dataset, Row}

import scala.util.{Failure, Success, Try}

case class ExportApp private[`export`](override val args: ExportArgs, dbProps: DbProps, pathProvider: SparkSessionType[String], output: FlowOutput[Row, Unit, Dataset])
  extends ExecutedApp[ExportArgs](args) {
  override def start(): Unit = {
    val sparkSession = createSparkSessionBuilder().getOrCreate()

    val eventFlow: SparkSessionType[Row] = pathProvider.map((ds: Dataset[String]) => {
      ds.collect().map(path => {
        Try(sparkSession.read.format("parquet").load(path)) match {
          case Success(p) => p
          case Failure(e) => throw e
        }
      }).reduce((p1: Dataset[Row], p2: Dataset[Row]) => p1.union(p2))
    })
    val dbFlow: SparkSessionType[Row] = VarEngDbFlow(dbProps, args)
    val mergeFlow: FromDatasetPairFlow[Row, Row, Row] = new FromDatasetPairFlow[Row, Row, Row]((eventDataset, dbDataset) => eventDataset.join(dbDataset))
    val flow = new FlowFromDatasetPairFlow[Row, Row, Row](eventFlow, dbFlow, mergeFlow)
    output.output(flow.execution(createSparkSessionBuilder().getOrCreate()))
  }
}

object ExportApp {

}