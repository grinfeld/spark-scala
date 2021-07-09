package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.helpers

import infra.spark.DatasetTypes.SparkSessionRowType

import org.apache.spark.sql.SparkSession

object ReadFromFilesFlow {
  def apply(format: String, paths:String*): SparkSessionRowType =
    DatasetFlow.createFromSession((sp: SparkSession) => sp.read.format(format).load(paths:_*))
}
