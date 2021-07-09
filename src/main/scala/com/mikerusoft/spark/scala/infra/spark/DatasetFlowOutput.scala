package com.mikerusoft.spark.scala.infra.spark

import com.mikerusoft.spark.scala.infra.FlowOutput
import org.apache.spark.sql.{Dataset, SaveMode}

import scala.collection.JavaConverters._


trait DatasetFlowOutput[C, O] extends FlowOutput[C, O, Dataset]

class ParquetWriterOutput[C](val pathTo: String, val saveMode: SaveMode, val writeCoalesce: Option[Int], val colNames: String*) extends DatasetFlowOutput[C, Unit] {
  override def output(dataset: Dataset[C]): Unit = {
    (writeCoalesce match {
      case Some(numPartitions) => dataset.coalesce(numPartitions)
      case None => dataset
    }).write.mode(saveMode).partitionBy(colNames:_*).save(pathTo)
  }
}

class ListWriterOutput[C] extends DatasetFlowOutput[C, List[C]] {
  override def output(fc: Dataset[C]): List[C] = fc.collectAsList().asScala.toList
}
