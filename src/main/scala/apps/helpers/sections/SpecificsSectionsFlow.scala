package com.dy.spark.scala
package apps.helpers.sections

import infra.spark.DatasetFlow
import infra.spark.DatasetTypes.SparkSessionType

import org.apache.spark.sql.Encoders

object SpecificsSectionsFlow {
  def apply(sections: Int*): SparkSessionType[Int] = {
    DatasetFlow.createFromSession(sparkSession => sparkSession.createDataset(sections)(Encoders.scalaInt))
  }
}
