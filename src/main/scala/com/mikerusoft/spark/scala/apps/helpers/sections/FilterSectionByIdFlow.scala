package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.helpers.sections

import infra.spark.DatasetTypes.Dataset2RowType

import org.apache.spark.sql.{Dataset, Row}

object FilterSectionByIdFlow {
  def apply(withSectionListArgs: WithSectionListArgs): Dataset2RowType = {
    withSectionListArgs.specificSections match {
      case List() => DatasetFlow.createFromDataset((ds: Dataset[Row]) => ds)
      case sections => DatasetFlow.createFromDataset((ds: Dataset[Row]) =>
        ds.filter(row => sections.contains(row.getAs("customerId")))
      )
    }
  }
}