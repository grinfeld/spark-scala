package com.dy.spark.scala
package infra.spark

import infra.spark.DatasetTypes.{DatasetType, SparkSessionType, Tuple2DatasetType}
import infra.{FPairFlow, Flow, FlowOutput}

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

trait DatasetFlow[I, C] extends Flow[I, C, Dataset]

class FromDatasetFlow[I, C](val execution: Dataset[I] => Dataset[C]) extends DatasetType[I, C] {
  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): DatasetType[I, B] =
    new FromDatasetFlow[I, B]((i: Dataset[I]) => flow.execution(this.execution(i)))

  override def map[D](func: Dataset[C] => Dataset[D]): DatasetType[I, D] =
    this concat new FromDatasetFlow[C, D](func)
}

class StartFlow[C](val execution: SparkSession => Dataset[C]) extends SparkSessionType[C] {
  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): SparkSessionType[B] =
    new StartFlow[B](sparkSession => flow.execution(this.execution(sparkSession)))

  override def map[D](func: Dataset[C] => Dataset[D]): SparkSessionType[D] =
    this concat new FromDatasetFlow[C, D](func)
}

class PairStartFlow[A1, A2, C](val start: SparkSession => (Dataset[A1],Dataset[A2]), val combiner: (Dataset[A1],Dataset[A2]) => Dataset[C]) extends SparkSessionType[C] {
  override def execution: SparkSession => Dataset[C] = sparkSession => {
    val tuple = start(sparkSession)
    combiner(tuple._1, tuple._2)
  }

  override def map[D](func: Dataset[C] => Dataset[D]): SparkSessionType[D] =
    this concat new FromDatasetFlow[C, D](func)

  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): SparkSessionType[B] =
    new StartFlow[B](sparkSession => flow.execution(this.execution(sparkSession)))
}

/*class FlowFromDatasetPairFlow[A1, A2, C](val start: () => (StartFlow[A1],StartFlow[A2]), val combiner: (Dataset[A1],Dataset[A2]) => Dataset[C]) extends SparkSessionType[C] {
  override def execution: SparkSession => Dataset[C] = sparkSession => {
    val tuple = start()
    val flow1 = tuple._1
    val flow2 = tuple._2
    combiner(flow1.execution(sparkSession), flow2.execution(sparkSession))
  }

  override def map[D](func: Dataset[C] => Dataset[D]): SparkSessionType[D] =
    this concat new FromDatasetFlow[C, D](func)

  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): SparkSessionType[B] =
    new StartFlow[B](sparkSession => flow.execution(this.execution(sparkSession)))
}*/

class FlowFromDatasetPairFlow[A1, A2, C](val first: SparkSessionType[A1], val second: SparkSessionType[A2], val combiner: FromDatasetPairFlow[A1, A2, C]) extends SparkSessionType[C] {
  override def execution: SparkSession => Dataset[C] = sparkSession => combiner.execution((first.execution(sparkSession), second.execution(sparkSession)))

  override def map[D](func: Dataset[C] => Dataset[D]): SparkSessionType[D] =
    this concat new FromDatasetFlow[C, D](func)

  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): SparkSessionType[B] =
    new StartFlow[B](sparkSession => flow.execution(this.execution(sparkSession)))
}

class FromDatasetPairFlow[I1, I2, C](override val func: (Dataset[I1], Dataset[I2]) => Dataset[C])
                                  extends FPairFlow[I1, I2, C, Dataset](func) {
  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): Tuple2DatasetType[I1, I2, B] =
      new FromDatasetPairFlow[I1, I2, B]((f1: Dataset[I1], f2:Dataset[I2]) => flow.execution(this.execution((f1, f2)))) {}

  override def map[D](func: Dataset[C] => Dataset[D]): Tuple2DatasetType[I1, I2, D] =
    this concat new FromDatasetFlow[C, D](func)

}

trait DatasetFlowOutput[C, O] extends FlowOutput[C, O, Dataset]

class ParquetWriterOutput[C](val pathTo: String, val saveMode: SaveMode, val writeCoalesce: Option[Int], val colNames: String*) extends DatasetFlowOutput[C, Unit] {
  override def output(dataset: Dataset[C]): Unit = {
    (writeCoalesce match {
      case Some(numPartitions) => dataset.coalesce(numPartitions)
      case None => dataset
    }).write.mode(saveMode).partitionBy(colNames:_*).save(pathTo)
  }
}

object DatasetFlow {
  def createFromSession[C](func: SparkSession => Dataset[C]): SparkSessionType[C] = new StartFlow[C](func)
  def createFromDataset[I, C](func: Dataset[I] => Dataset[C]): DatasetType[I, C] = new FromDatasetFlow[I,C](func)
}