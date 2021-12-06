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
  case class Holder[A1, A2] private (first: Option[SparkSessionType[A1]], second: Option[SparkSessionType[A2]]) {
    def this() = this(None, None)
    def withFirstFlow(a1:SparkSessionType[A1]): Holder[A1, A2] = copy(first = Option(a1))
    def withSecondFlow(a2:SparkSessionType[A2]): Holder[A1, A2] = copy(second = Option(a2))
    def combine[C](combiner: (Dataset[A1], Dataset[A2]) => Dataset[C]): PairStartFlowToDatasetFlow[A1, A2, C] = {
      (for {
        f1 <- first
        f2 <- second
      } yield (f1, f2)) match {
        case None => throw new IllegalArgumentException // maybe better to use Either
        case Some(starts) => new PairStartFlowToDatasetFlow(starts._1, starts._2, FromDatasetPairFlow(combiner))
      }
    }
  }

  def withFirstFlow[A1, A2](first: SparkSessionType[A1]): Holder[A1, A2] = new Holder[A1, A2]().withFirstFlow(first)

  def apply[A1, A2, C](first: SparkSessionType[A1], second: SparkSessionType[A2], combiner: (Dataset[A1], Dataset[A2]) => Dataset[C]): SparkSessionType[C] =
    new PairStartFlowToDatasetFlow(first, second, FromDatasetPairFlow(combiner))
}
