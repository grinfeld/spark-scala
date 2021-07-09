package com.mikerusoft.spark.scala.apps.helpers

import com.mikerusoft.spark.scala.infra.spark.DatasetFlow
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.SparkSessionRowType
import org.apache.spark.sql.SparkSession

object ReadFromFilesFlow {
  def apply(format: String, paths:String*): SparkSessionRowType =
    DatasetFlow.createFromSession((sp: SparkSession) => sp.read.format(format).load(paths:_*))
}
