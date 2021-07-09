package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.helpers.sections

import infra.spark.DatasetTypes.SparkSessionType

import org.apache.spark.sql.Encoders

object SpecificsSectionsFlow {
  def apply(sections: Int*): SparkSessionType[Int] = {
    DatasetFlow.createFromSession(sparkSession => sparkSession.createDataset(sections)(Encoders.scalaInt))
  }
}
