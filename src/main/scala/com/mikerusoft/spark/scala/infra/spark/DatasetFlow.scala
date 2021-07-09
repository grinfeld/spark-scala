package com.mikerusoft.spark.scala.infra.spark


import com.mikerusoft.spark.scala.infra.Flow
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.{DatasetType, SparkSessionType}
import org.apache.spark.sql.{Dataset, SparkSession}

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

object DatasetFlow {
  def createFromSession[C](func: SparkSession => Dataset[C]): SparkSessionType[C] = new StartFlow[C](func)
  def createFromDataset[I, C](func: Dataset[I] => Dataset[C]): DatasetType[I, C] = new FromDatasetFlow[I,C](func)
}