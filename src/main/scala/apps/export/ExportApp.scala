package com.dy.spark.scala
package apps.`export`

import apps.ExecutedApp
import apps.`export`.db.{S3ExportPathsBySectionFlow, VarEngDbFlow}
import apps.helpers.db.DbProps
import infra.FlowOutput
import infra.spark.DatasetTypes.SparkSessionType
import infra.spark.{FlowFromDatasetPairFlow, FromDatasetPairFlow, ParquetWriterOutput}

import org.apache.spark.sql.{Dataset, Row, SaveMode}

import scala.util.{Failure, Success, Try}

/**
 *
 * @param args app properties
 * @param pathProvider the way to create paths for spark.read.format(..).load
 * @param dbFlow describes getting meta data from DB (with spark)
 * @param output describes the output
 */
case class ExportApp private[`export`](override val args: ExportArgs,
       pathProvider: SparkSessionType[String], dbFlow: SparkSessionType[Row], output: FlowOutput[Row, Unit, Dataset])
  extends ExecutedApp[ExportArgs](args) {
  override def start(): Unit = {
    val sparkSession = createSparkSessionBuilder().getOrCreate()

    // pathProvider - the way to create paths from s3, could be dependant on getting some data from DB (via spark, of course)
    // dbFlow - getting data from DB meta data

    // defines the way we read data from s3
    val eventFlow: SparkSessionType[Row] = pathProvider.map((ds: Dataset[String]) => {
      ds.collect().map(path => {
        Try(sparkSession.read.format("parquet").load(path)) match {
          case Success(p) => p
          case Failure(e) => throw e
        }
      }).reduce((p1: Dataset[Row], p2: Dataset[Row]) => p1.union(p2))
    })

    // how to to merge 2 flows (should be defined outside of this class, maybe?)
    val mergeFlow: FromDatasetPairFlow[Row, Row, Row] = new FromDatasetPairFlow[Row, Row, Row]((eventDataset, dbDataset) => eventDataset.join(dbDataset))

    // final flow - encapsulated the whole logic
    val finalFlow = new FlowFromDatasetPairFlow[Row, Row, Row](eventFlow, dbFlow, mergeFlow)

    // building output and initiating
    output.output(finalFlow.execution(createSparkSessionBuilder().getOrCreate()))
  }
}

object ExportApp {
  def apply(args: ExportArgs, dbProps: DbProps): ExportApp = {
    new ExportApp(args,
      args.specificSection match {
        case None => S3ExportPathsBySectionFlow(args.actualDate)
        case Some(sectionId) => S3ExportPathsBySectionFlow(dbProps, sectionId, args.actualDate)
      },
      VarEngDbFlow(dbProps, args.specificSection, args.offsetHour),
      new ParquetWriterOutput[Row](args.outputPath, SaveMode.Overwrite, None, "sectionId")
    )
  }
}