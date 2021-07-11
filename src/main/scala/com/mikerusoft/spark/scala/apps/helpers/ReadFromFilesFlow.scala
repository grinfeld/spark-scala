package com.mikerusoft.spark.scala.apps.helpers

import com.mikerusoft.spark.scala.infra.spark.DatasetFlow
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.SparkSessionRowType

object ReadFromFilesFlow {
  def apply(format: String, paths:String*): SparkSessionRowType =
    DatasetFlow.createFromSession(_.read.format(format).load(paths:_*))
}
