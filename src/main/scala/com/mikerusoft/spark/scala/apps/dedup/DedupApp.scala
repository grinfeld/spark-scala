package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.dedup

import infra.spark.DatasetTypes.{Dataset2RowType, SparkSessionRowType}
import infra.FlowOutput

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, functions}

case class DedupApp private[dedup] (override val args: DedupArgs, startFlow: SparkSessionRowType, filterSectionsFlow: Dataset2RowType,
                    output: FlowOutput[Row, Unit, Dataset]) extends ExecutedApp[DedupArgs](args) {

  private val DEDUP_FIELDS: Array[String] = Array[String]("customerId", "userId", "eventType", "rri")

  override def start(): Unit = {
    val flow: Flow[SparkSession, Row, Dataset] = startFlow.concat(filterSectionsFlow)
      .map(dataset => dataset.repartition(functions.col("customerId"))
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
      new ParquetWriterOutput[Row](args.outputPath, SaveMode.Overwrite, None, "customerId")
  )
}