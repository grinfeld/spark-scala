package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.`export`.db

import apps.`export`.model.spark.RawEventV2ExportTransformer.SparkEncoder
import apps.helpers.db.DbProps
import infra.spark.DatasetTypes.SparkSessionType

import org.apache.spark.sql.functions.{col, collect_list, concat, lit}

object VarEngDbFlow {

  private val SPLIT_CHAR = "~"

  def apply(dbProps: DbProps, customerId: Option[Int], offsetHour: Int): SparkSessionType[RawEventV2Export] = {
    DbDatasetFlow(dbProps, buildQuery(customerId, offsetHour)).map(dataset => {
        dataset.withColumn("variation_pair", concat(col("variation_performance_id"), lit(SPLIT_CHAR), col("variation_id"), lit(SPLIT_CHAR), col("variation_name")))
          .groupBy("section_id", "experience_id", "experience_name", "experiment_id", "experiment_version_id", "campaign_id", "campaign_name")
          .agg(collect_list("variation_pair"))
          .withColumnRenamed("collect_list(variation_pair)", "variation_names")
          .map(RawEventV2ExportTransformer.transformRow)
      }
    )
  }

  private def buildQuery(customerId: Option[Int], offsetHour: Int): String = {
    customerId match {
      case None => offsetQuery.format(offsetHour)
      case Some(num) => sectionQuery.format(num)
    }
  }

  private val query = """SELECT
    so.id as experience_id,
    so.name as experience_name,
    sov.id AS variation_id,
    sov.name AS variation_name,
    ver.id AS experiment_version_id,
    evar.id AS variation_performance_id,
    so.section_id,
    so.experiment_id,
    st.id campaign_id,
    st.display_name campaign_name
      FROM
    dypd_sections sec

    JOIN section_exports as secex
      ON secex.section_id = sec.id

    JOIN experiments as ex
      ON ex.section_id = sec.id

    JOIN
    experiment_versions as ver
    ON ex.id = ver.experiment_id

    JOIN
    experiment_variations_experiment_versions evev
      ON ver.id = evev.experiment_version_id

    JOIN
    experiment_variations evar
      on evev.experiment_variation_id = evar.id

    JOIN
    smart_objects so
      ON so.id = ex.parent_id

    JOIN
    smart_object_variations sov
      ON sov.id = evar.smart_object_variation_id
    and sov.name not like "_dy_collection_inject"
    and sov.name not like "_dy_qa_action"

    JOIN
    smart_tag_objects sto
      ON sto.smart_object_id = sov.smart_object_id
    JOIN
    smart_tags st
      ON sto.smart_tag_id = st.id

    WHERE
    st.name not like "multi_touch%"
    and secex.active = 1 and secex.export_type like "raw_data"
    and ex.section_id != 8767379"""

    private val offsetQuery = s"($query and sec.time_zone_offset = %s)tmp"
    private val sectionQuery = s"($query and sec.id = %s)tmp"
}
