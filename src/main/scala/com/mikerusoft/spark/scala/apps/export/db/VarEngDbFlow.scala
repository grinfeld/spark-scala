package com.mikerusoft.spark.scala.apps.`export`.db

import com.mikerusoft.spark.scala.apps.`export`.model.IncomingEventExport
import com.mikerusoft.spark.scala.apps.`export`.model.spark.IncomingEventExportTransformer
import com.mikerusoft.spark.scala.apps.`export`.model.spark.IncomingEventExportTransformer.SparkEncoder
import com.mikerusoft.spark.scala.apps.helpers.db.{DbDatasetFlow, DbProps}
import com.mikerusoft.spark.scala.infra.spark.DatasetTypes.SparkSessionType
import org.apache.spark.sql.functions.{col, collect_list, concat, lit}

object VarEngDbFlow {

  private val SPLIT_CHAR = "~"

  def apply(dbProps: DbProps, customerId: Option[Int], offsetHour: Int): SparkSessionType[IncomingEventExport] = {
    DbDatasetFlow(dbProps, buildQuery(customerId, offsetHour)).map(
        _.withColumn("variation_pair", concat(col("variation_performance_id"), lit(SPLIT_CHAR), col("variation_id"), lit(SPLIT_CHAR), col("variation_name")))
          .groupBy("customer_id", "experience_id", "experience_name", "experiment_id", "experiment_version_id", "campaign_id", "campaign_name")
          .agg(collect_list("variation_pair"))
          .withColumnRenamed("collect_list(variation_pair)", "variation_names")
          .map(IncomingEventExportTransformer.transformRow)
    )
  }

  private def buildQuery(customerId: Option[Int], offsetHour: Int): String = {
    customerId match {
      case None => offsetQuery.format(offsetHour)
      case Some(num) => sectionQuery.format(num)
    }
  }

  private val query = """SELECT
    basic query here
    where
    """

    private val offsetQuery = s"($query and sec.time_zone_offset = %s)tmp"
    private val sectionQuery = s"($query and sec.id = %s)tmp"
}
