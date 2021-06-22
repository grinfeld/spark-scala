package com.dy.spark.scala
package apps.helpers

import infra.spark.DatasetFlow
import infra.spark.DatasetTypes.SparkSessionRowType

import org.apache.spark.sql.SparkSession

object ReadFromFilesFlow {
  def apply(format: String, paths:String*): SparkSessionRowType =
    DatasetFlow.createFromSession((sp: SparkSession) => sp.read.format(format).load(paths:_*))
}
