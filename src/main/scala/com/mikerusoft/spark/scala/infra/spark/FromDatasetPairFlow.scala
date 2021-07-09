package com.mikerusoft.spark.scala.infra.spark

import DatasetTypes.Tuple2DatasetType
import com.mikerusoft.spark.scala.infra.{FPairFlow, Flow}
import org.apache.spark.sql.Dataset

class FromDatasetPairFlow[I1, I2, C](override val func: (Dataset[I1], Dataset[I2]) => Dataset[C])
  extends FPairFlow[I1, I2, C, Dataset](func) {
  override def concat[B](flow: Flow[Dataset[C], B, Dataset]): Tuple2DatasetType[I1, I2, B] =
    new FromDatasetPairFlow[I1, I2, B]((f1: Dataset[I1], f2: Dataset[I2]) => flow.execution(this.execution((f1, f2)))) {}

  override def map[D](func: Dataset[C] => Dataset[D]): Tuple2DatasetType[I1, I2, D] =
    this concat new FromDatasetFlow[C, D](func)

}

object FromDatasetPairFlow {
  def apply[I1, I2, C](func: (Dataset[I1], Dataset[I2]) => Dataset[C]): FPairFlow[I1, I2, C, Dataset] = new FromDatasetPairFlow(func)
}
