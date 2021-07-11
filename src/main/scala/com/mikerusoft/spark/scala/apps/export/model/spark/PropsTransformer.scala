package com.mikerusoft.spark.scala.apps.`export`.model.spark

import com.mikerusoft.spark.scala.apps.`export`.model.{PRODUCT, IncomingEventExport, RecommendationContextType}
import com.mikerusoft.spark.scala.infra.spark.RowWrapper.RowWrapper
import com.mikerusoft.spark.scala.model.gen.enums._
import org.apache.spark.sql.Row

trait PropsTransformer[T] {
  def transform(tp: T, row: Row, copyTo: IncomingEventExport): IncomingEventExport
}

object PropsTransformer {

  implicit class EventTypeTransformer[T <: RawEventType](tp: T) {
    def transform(row: Row, copyTo: IncomingEventExport)(implicit transformer: PropsTransformer[T]) : IncomingEventExport =
      transformer.transform(tp, row, copyTo)
  }

  implicit object PropsTransformer extends PropsTransformer[RawEventType] {
    override def transform(tp: RawEventType, row: Row, copyTo: IncomingEventExport): IncomingEventExport =
      tp match {
        case v: EVENT => v.transform(row, copyTo)
        case v: VIEW => v.transform(row, copyTo)
        case v: VAR => v.transform(row, copyTo)
        case v: IDENTIFY => v.transform(row, copyTo)
        case _: NA => copyTo
      }
  }

  implicit object EventPropsTransformer extends PropsTransformer[EVENT] {
    val REMOVE_FROM_CART_V1 = "remove-from-cart-v1"
    val ADD_TO_WISHLIST_V1 = "add-to-wishlist-v1"
    val ADD_TO_CART_V1 = "add-to-cart-v1"
    val PURCHASE_V1 = "purchase-v1"
    val SYNC_CART_V1 = "sync-cart-v1"
    val SYNC_WISH_LIST_V1 = "sync-wishlist-v1"

    override def transform(tp: EVENT, row: Row, copyTo: IncomingEventExport): IncomingEventExport =
      copyTo.copy(
        eventId = row.getAsOption("eventId"),
        eventName = row.getAsOption("eventName"),
        eventValue = row.getAsOption("value")
      )
  }

  implicit object ViewPropsTransformer extends PropsTransformer[VIEW] {
    override def transform(tp: VIEW, row: Row, copyTo: IncomingEventExport): IncomingEventExport =
      row.getAsOption[Row]("pageContext") match {
        case None => copyTo
        case Some(ctx) =>
          val contextType = RecommendationContextType(ctx.getAsOption("typeOrdinal"))
          val contextData = row.getAsList[String]("data")
          copyTo.copy(
            contextType = Option(contextType.name),
            contextData = contextData,
            productIds = contextType match {
              case _: PRODUCT => contextData
              case _ => List()
            }
          )
      }
  }

  implicit object VarPropsTransformer extends PropsTransformer[VAR] {
    override def transform(tp: VAR, row: Row, copyTo: IncomingEventExport): IncomingEventExport = {
      val experimentMetadataRow: Row = row.getAs("experimentMetadata")
      copyTo.copy(
        engagementType = row.getAsOption("experimentEngagementType"),
        experienceId = experimentMetadataRow.getAsOption("experimentId"),
        versionId = experimentMetadataRow.getAsOption("versionId"),
        variationIds = experimentMetadataRow.getAsList("variations")
      )
    }
  }

  implicit object IdentifyPropsTransformer extends PropsTransformer[IDENTIFY] {
    override def transform(tp: IDENTIFY, row: Row, copyTo: IncomingEventExport): IncomingEventExport = copyTo
  }
}
