package com.mikerusoft.spark.scala.apps.`export`.model.spark

import PropsTransformer._
import com.mikerusoft.spark.scala.apps.`export`.model.IncomingEventExport
import com.mikerusoft.spark.scala.infra.spark.RowWrapper.RowWrapper
import com.mikerusoft.spark.scala.model.gen.enums._
import org.apache.spark.sql.{Encoder, Encoders, Row}

object IncomingEventExportTransformer {
  implicit val SparkEncoder: Encoder[IncomingEventExport] = Encoders.product[IncomingEventExport]

  implicit class RawEventV2ExportWrapper(exp: IncomingEventExport) {
    def rawEventType: Option[RawEventType] = {
      exp.eventType match {
        case None => None
        case Some(tp) => Some(RawEventType(tp.toUpperCase))
      }
    }

    def transform[T <: RawEventType](row: Row): IncomingEventExport = {
      rawEventType match {
        case None => exp
        case Some(tp) => tp.transform(row, exp)
      }
    }
  }

  def transformRow: Row => IncomingEventExport = { implicit row => {
      IncomingEventExport(row.getAsOption("section_id"), row.getAsList("variation_names"),
        row.getAsOption("experiment_id"), row.getAsOption("experience_id"), row.getAsOption("experience_name"),
        row.getAsOption("experiment_version_id"), row.getAsOption("campaign_id"), row.getAsOption("campaign_name")
      ).copy(eventType = row.getAsOption[String]("eventType")).transform(row)
    }
  }
}
