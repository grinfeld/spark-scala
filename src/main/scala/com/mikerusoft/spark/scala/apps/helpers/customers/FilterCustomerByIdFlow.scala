package com.mikerusoft.spark.scala.apps.helpers.customers

import com.mikerusoft.spark.scala.infra.spark.DatasetFlow
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.Dataset2RowType
import org.apache.spark.sql.{Dataset, Row}

object FilterCustomerByIdFlow {
  def apply(withSectionListArgs: WithCustomerListArgs): Dataset2RowType = {
    withSectionListArgs.specificSections match {
      case List() => DatasetFlow.createFromDataset((ds: Dataset[Row]) => ds)
      case sections => DatasetFlow.createFromDataset((ds: Dataset[Row]) =>
        ds.filter(row => sections.contains(row.getAs("customerId")))
      )
    }
  }
}