package com.dy.spark.scala
package apps.`export`.model.spark

import apps.`export`.model.RawEventV2Export
import apps.`export`.model.spark.PropsTransformer._
import infra.spark.RowWrapper.RowWrapper
import model.gen.raweventtype._

import org.apache.spark.sql.{Encoder, Encoders, Row}

object RawEventV2ExportTransformer {
  implicit val SparkEncoder: Encoder[RawEventV2Export] = Encoders.product[RawEventV2Export]

  implicit class RawEventV2ExportWrapper(exp: RawEventV2Export) {
    def rawEventType: Option[RawEventType] = {
      exp.eventType match {
        case None => None
        case Some(tp) => Some(RawEventType(tp.toUpperCase))
      }
    }

    def transform[T <: RawEventType](row: Row): RawEventV2Export = {
      rawEventType match {
        case None => exp
        // since, we don't have actual "new", so compiler doesn't know what "implicit" to use, so seems better to use simple inheritance,
        // instead ad-hoc here
        case Some(tp) => tp match {
          case v: DPX => v.transform(row, exp)
          case v: UIA => v.transform(row, exp)
          case v: UNIT_CLICK => v.transform(row, exp)
          case v: UNIT_IMP => v.transform(row, exp)
          case v: VARIATION_ENGAGEMENT => v.transform(row, exp)
          case v: EXPERIMENT_ENGAGEMENT => v.transform(row, exp)
          case v: SMARTLINK_CLICK => v.transform(row, exp)
          case v: ID_CUID => v.transform(row, exp)
        }
      }
    }
  }

  def transformRow: Row => RawEventV2Export = { implicit row => {
      RawEventV2Export(row.getAsOption("section_id"), row.getAsList("variation_names"),
        row.getAsOption("experiment_id"), row.getAsOption("experience_id"), row.getAsOption("experience_name"),
        row.getAsOption("experiment_version_id"), row.getAsOption("campaign_id"), row.getAsOption("campaign_name")
      ).copy(eventType = row.getAsOption[String]("eventType")).transform(row)
    }
  }
}
