package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.`export`.model.spark

import PropsTransformer._
import infra.spark.RowWrapper.RowWrapper
import model.gen.enums._

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
        // since, we don't receive implicit type of variable, but "parent" type,
        // so compiler doesn't know what "implicit" to use, so seems better to use simple inheritance or similar,
        // instead ad-hoc inheritance here
        case Some(tp) => tp match {
          case v: EVENT => v.transform(row, exp)
          case v: VIEW => v.transform(row, exp)
          case v: VAR => v.transform(row, exp)
          case v: IDENTIFY => v.transform(row, exp)
          case v: NA => exp
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
