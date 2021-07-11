package com.mikerusoft.spark.scala.apps.`export`

import com.mikerusoft.spark.scala.apps.ExecutedApp
import com.mikerusoft.spark.scala.apps.`export`.db.{S3ExportPathsByCustomerFlow, VarEngDbFlow}
import com.mikerusoft.spark.scala.apps.`export`.model.IncomingEventExport
import com.mikerusoft.spark.scala.apps.`export`.model.spark.IncomingEventExportTransformer
import com.mikerusoft.spark.scala.apps.`export`.model.spark.IncomingEventExportTransformer.SparkEncoder
import com.mikerusoft.spark.scala.apps.helpers.db.DbProps
import com.mikerusoft.spark.scala.infra.FlowOutput
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.SparkSessionType
import com.mikerusoft.spark.scala.infra.spark.{PairStartFlowToDatasetFlow, ParquetWriterOutput}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 *
 * @param args app properties
 * @param pathProvider the way to create paths for spark.read.format(..).load
 * @param dbFlow describes getting meta data from DB (with spark)
 * @param output describes the output
 */
case class ExportApp private[`export`](override val args: ExportArgs,
                                       pathProvider: SparkSessionType[String], dbFlow: SparkSessionType[IncomingEventExport], output: FlowOutput[Row, Unit, Dataset])
  extends ExecutedApp[ExportArgs](args) {
  override def start(): Unit = {
    val sparkSession: SparkSession = createSparkSessionBuilder().getOrCreate()

    // pathProvider - the way to create paths from s3, could be dependant on getting some data from DB (via spark, of course)
    // dbFlow - getting data from DB meta data

    // defines the way we read data from s3
    val eventFlow = pathProvider.map((ds: Dataset[String]) => createInputDataset(ds, sparkSession))

    // final flow - encapsulated the whole logic
    val finalFlow = PairStartFlowToDatasetFlow.withFirstFlow(eventFlow).withSecondFlow(dbFlow)
      .combine((eventDataset, dbDataset) => {
        val dbDatasetWithSelect = dbDataset.select("campaignId", "campaignName", "experienceName", "experienceId", "experimentId", "customerId", "versionId", "variationNames")
        val varEngs = eventDataset.filter(r => r.eventType.exists(t => t equals "ENGAGEMENT"))
          .drop("campaignId", "campaignName", "experienceName", "experienceId", "variationNames")
          .join(dbDatasetWithSelect, columns, "left")
        val noVarEng = eventDataset.filter(r => r.eventType.exists(t => !t.equals("ENGAGEMENT"))).selectExpr(columns:_*)
        noVarEng.union(varEngs)
    })

    // building output and initiating
    output.output(finalFlow.execution(createSparkSessionBuilder().getOrCreate()))
  }

  def createInputDataset(ds: Dataset[String], sparkSession: SparkSession): Dataset[IncomingEventExport] = {
    ds.collect().map(path => {
      Try(sparkSession.read.format("parquet").load(path)) match {
        case Success(p) => p
        case Failure(e) => throw e
      }
    })
      .reduce((p1: Dataset[Row], p2: Dataset[Row]) => p1.union(p2))
      .map(IncomingEventExportTransformer.transformRow)
  }

  private val columns = List("customerId", "experimentId", "versionId")
}

object ExportApp {
  def apply(args: ExportArgs, dbProps: DbProps): ExportApp = {
    new ExportApp(args,
      args.specificSection match {
        case None => S3ExportPathsByCustomerFlow(args.actualDate)
        case Some(customerId) => S3ExportPathsByCustomerFlow(dbProps, customerId, args.actualDate)
      },
      VarEngDbFlow(dbProps, args.specificSection, args.offsetHour),
      ParquetWriterOutput[Row](args.outputPath, SaveMode.Overwrite, None, "customerId")
    )
  }
}