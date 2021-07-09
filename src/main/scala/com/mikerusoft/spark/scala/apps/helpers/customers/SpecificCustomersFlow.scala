package com.mikerusoft.spark.scala.apps.helpers.customers

import com.mikerusoft.spark.scala.infra.spark.DatasetFlow
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.SparkSessionType
import org.apache.spark.sql.Encoders

object SpecificCustomersFlow {
  def apply(sections: Int*): SparkSessionType[Int] = {
    DatasetFlow.createFromSession(sparkSession => sparkSession.createDataset(sections)(Encoders.scalaInt))
  }
}
