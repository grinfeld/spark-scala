package com.mikerusoft.spark.scala.apps.`export`.db

import com.mikerusoft.spark.scala.apps.helpers.db.{DbDatasetFlow, DbProps}
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.SparkSessionType
import org.apache.spark.sql.Encoders

object ExportCustomersFlow {
  private val query = "select se.customer_id as customer_id\n" +
    "from customer_exports se\n" +
    "join customers sec on sec.id = se.customer_id\n" +
    "where se.export_type like 'raw_data' and se.active = 1 and sec.active = 1 and sec.time_zone_offset = "

  def apply(dbProps: DbProps, offset: Int): SparkSessionType[Int] = {
    DbDatasetFlow(dbProps, query + s"$offset").map(_.select("customer_id").as(Encoders.scalaInt))
  }
}
