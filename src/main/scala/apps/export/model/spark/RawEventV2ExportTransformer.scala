package com.dy.spark.scala
package apps.`export`.model.spark

import apps.`export`.model.RawEventV2Export
import infra.spark.RowWrapper.RowWrapper

import org.apache.spark.sql.{Encoder, Encoders, Row}

object RawEventV2ExportTransformer {
  implicit val SparkEncoder: Encoder[RawEventV2Export] = Encoders.bean[RawEventV2Export]

  def transformRow: Row => RawEventV2Export = { implicit row => {
      RawEventV2Export(row.getAsOption("section_id"), row.getAsList("variation_names"),
        row.getAsOption("experiment_id"), row.getAsOption("experience_id"), row.getAsOption("experience_name"),
        row.getAsOption("experiment_version_id"), row.getAsOption("campaign_id"), row.getAsOption("campaign_name")
      ).fillValue[String]("eventType", (r, v) => r.copy(eventType = Option(v)))
    }
  }

  private def fillNestedObjects(row: Row, rawExport: RawEventV2Export): RawEventV2Export = {
    rawExport
  }

  implicit class RawEventV2ExportWithRow(rawExport: RawEventV2Export) {
    def fillValue[T](name: String, copied: (RawEventV2Export, T) => RawEventV2Export)(implicit row: Row): RawEventV2Export = row.getAsOption(name).asInstanceOf[Option[T]] match {
      case Some(value) => copied(rawExport, value)
      case None => rawExport
    }
  }
}
