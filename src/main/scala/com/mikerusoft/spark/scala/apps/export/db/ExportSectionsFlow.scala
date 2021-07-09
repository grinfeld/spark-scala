package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.`export`.db

import apps.helpers.db.DbProps
import infra.spark.DatasetTypes.SparkSessionType

import org.apache.spark.sql.Encoders

object ExportSectionsFlow {
  private val query = "select se.section_id as section_id\n" +
    "from section_exports se\n" +
    "join dypd_sections sec on sec.id = se.section_id\n" +
    "where se.export_type like 'raw_data' and se.active = 1 and sec.active = 1 and sec.time_zone_offset = "

  def apply(dbProps: DbProps, offset: Int): SparkSessionType[Int] = {
    DbDatasetFlow(dbProps, query + s"$offset").map(ds => ds.select("section_id").as(Encoders.scalaInt))
  }
}
