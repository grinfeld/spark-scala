package com.mikerusoft.spark.scala.infra.spark

import DatasetTypes.SparkSessionType
import com.mikerusoft.spark.scala.infra.{FPairFlow, Flow}
import org.apache.spark.sql.{Dataset, SparkSession}

class PairStartFlowToDatasetFlow[A1, A2, C](val first: SparkSessionType[A1], val second: SparkSessionType[A2], val combiner: FPairFlow[A1, A2, C, Dataset]) extends SparkSessionType[C] {
  override def execution: SparkSession => Dataset[C] = sparkSession => combiner.execution((first.execution(sparkSession), second.execution(sparkSession)))

  override def map[D](func: Dataset[C] => Dataset[D]): SparkSessionType[D] =
    this concat new FromDatasetFlow[C, D](func)

  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): SparkSessionType[B] =
    new StartFlow[B](sparkSession => flow.execution(this.execution(sparkSession)))
}

object PairStartFlowToDatasetFlow {
  def apply[A1, A2, C](first: SparkSessionType[A1], second: SparkSessionType[A2], combiner: (Dataset[A1], Dataset[A2]) => Dataset[C]): SparkSessionType[C] =
    new PairStartFlowToDatasetFlow(first, second, FromDatasetPairFlow(combiner))
}
