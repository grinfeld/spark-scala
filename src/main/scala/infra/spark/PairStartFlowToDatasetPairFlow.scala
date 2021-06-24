package com.dy.spark.scala
package infra.spark

import infra.{FPairFlow, Flow}
import infra.spark.DatasetTypes.SparkSessionType

import org.apache.spark.sql.{Dataset, SparkSession}

class PairStartFlowToDatasetPairFlow[A1, A2, C](val first: SparkSessionType[A1], val second: SparkSessionType[A2], val combiner: FPairFlow[A1, A2, C, Dataset]) extends SparkSessionType[C] {
  override def execution: SparkSession => Dataset[C] = sparkSession => combiner.execution((first.execution(sparkSession), second.execution(sparkSession)))

  override def map[D](func: Dataset[C] => Dataset[D]): SparkSessionType[D] =
    this concat new FromDatasetFlow[C, D](func)

  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): SparkSessionType[B] =
    new StartFlow[B](sparkSession => flow.execution(this.execution(sparkSession)))
}

object PairStartFlowToDatasetPairFlow {
  def apply[A1, A2, C](first: SparkSessionType[A1], second: SparkSessionType[A2], combiner: (Dataset[A1], Dataset[A2]) => Dataset[C]): SparkSessionType[C] =
    new PairStartFlowToDatasetPairFlow(first, second, FromDatasetPairFlow(combiner))
}
