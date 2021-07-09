package com.dy.spark.scala
package apps.dedup

import apps.ExecutedApp
import apps.helpers.ReadFromFilesFlow
import apps.helpers.sections.FilterSectionByIdFlow
import infra.spark.DatasetTypes.{Dataset2RowType, SparkSessionRowType}
import infra.{Flow, FlowOutput}

import com.dy.spark.scala.infra.spark.ParquetWriterOutput
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, functions}

case class DedupApp private[dedup] (override val args: DedupArgs, startFlow: SparkSessionRowType, filterSectionsFlow: Dataset2RowType,
                    output: FlowOutput[Row, Unit, Dataset]) extends ExecutedApp[DedupArgs](args) {

  private val DEDUP_FIELDS: Array[String] = Array[String]("sectionId", "dyid", "eventType", "rri")

  override def start(): Unit = {
    val flow: Flow[SparkSession, Row, Dataset] = startFlow.concat(filterSectionsFlow)
      .map(dataset => dataset.repartition(functions.col("sectionId"))
        .dropDuplicates(DEDUP_FIELDS)
        .withColumn("date", functions.lit(args.onlyDate)).withColumn("hour", functions.lit(args.onlyHour)))
    output.output(flow.execution(createSparkSessionBuilder().getOrCreate()))
  }
}

object DedupApp {
  def apply(args: DedupArgs) = new DedupApp(
      args,
      ReadFromFilesFlow(args.inputFormat.getOrElse("avro"), args.inputPath),
      FilterSectionByIdFlow(args),
      new ParquetWriterOutput[Row](args.outputPath, SaveMode.Overwrite, None, "sectionId")
  )
}
