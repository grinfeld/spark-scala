package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.`export`

import apps.`export`.db.VarEngDbFlow
import apps.`export`.model.spark.RawEventV2ExportTransformer.SparkEncoder
import infra.spark.DatasetTypes.SparkSessionType
import infra.spark.PairStartFlowToDatasetFlow

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
       pathProvider: SparkSessionType[String], dbFlow: SparkSessionType[RawEventV2Export], output: FlowOutput[Row, Unit, Dataset])
  extends ExecutedApp[ExportArgs](args) {
  override def start(): Unit = {
    val sparkSession = createSparkSessionBuilder().getOrCreate()

    // pathProvider - the way to create paths from s3, could be dependant on getting some data from DB (via spark, of course)
    // dbFlow - getting data from DB meta data

    // defines the way we read data from s3
    val eventFlow = pathProvider.map((ds: Dataset[String]) => {
      ds.collect().map(path => {
        Try(sparkSession.read.format("parquet").load(path)) match {
          case Success(p) => p
          case Failure(e) => throw e
        }
      })
        .reduce((p1: Dataset[Row], p2: Dataset[Row]) => p1.union(p2))
        .map(RawEventV2ExportTransformer.transformRow)
    })

    // final flow - encapsulated the whole logic
    val finalFlow = PairStartFlowToDatasetFlow[RawEventV2Export, RawEventV2Export, Row](
      eventFlow,
      dbFlow,
      (eventDataset, dbDataset) => {
        val dbDatasetWithSelect = dbDataset.select("campaignId", "campaignName", "experienceName", "experienceId", "experimentId", "customerId", "versionId", "variationNames")

        val varEngs = eventDataset.filter(r => r.eventType.exists(t => t equals "ENGAGEMENT"))
          .drop("campaignId", "campaignName", "experienceName", "experienceId", "variationNames")
          .join(dbDatasetWithSelect, columns, "left")
        val noVarEng = eventDataset.filter(r => r.eventType.exists(t => !t.equals("ENGAGEMENT"))).selectExpr(columns:_*)
        noVarEng.union(varEngs)
      }
    )

    // building output and initiating
    output.output(finalFlow.execution(createSparkSessionBuilder().getOrCreate()))
  }

  private val columns = List("customerId", "experimentId", "versionId")
}

object ExportApp {
  def apply(args: ExportArgs, dbProps: DbProps): ExportApp = {
    new ExportApp(args,
      args.specificSection match {
        case None => S3ExportPathsBySectionFlow(args.actualDate)
        case Some(customerId) => S3ExportPathsBySectionFlow(dbProps, customerId, args.actualDate)
      },
      VarEngDbFlow(dbProps, args.specificSection, args.offsetHour),
      new ParquetWriterOutput[Row](args.outputPath, SaveMode.Overwrite, None, "customerId")
    )
  }
}