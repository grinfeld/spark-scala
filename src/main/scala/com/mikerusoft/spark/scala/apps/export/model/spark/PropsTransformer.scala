package com.mikerusoft.spark.scala.apps.`export`.model.spark

import com.mikerusoft.spark.scala.apps.`export`.model.{PRODUCT, RawEventV2Export, RecommendationContextType}
import com.mikerusoft.spark.scala.infra.spark.RowWrapper.RowWrapper
import com.mikerusoft.spark.scala.model.gen.enums._
import org.apache.spark.sql.Row

trait PropsTransformer[T] {
  def transform(tp: T, row: Row, copyTo: RawEventV2Export): RawEventV2Export
}

object PropsTransformer {

  implicit class EventTypeTransformer[T <: RawEventType](tp: T) {
    def transform(row: Row, copyTo: RawEventV2Export)(implicit transformer: PropsTransformer[T]) : RawEventV2Export =
      transformer.transform(tp, row, copyTo)
  }

  implicit object DpxPropsTransformer extends PropsTransformer[EVENT] {
    val REMOVE_FROM_CART_V1 = "remove-from-cart-v1"
    val ADD_TO_WISHLIST_V1 = "add-to-wishlist-v1"
    val ADD_TO_CART_V1 = "add-to-cart-v1"
    val PURCHASE_V1 = "purchase-v1"
    val SYNC_CART_V1 = "sync-cart-v1"
    val SYNC_WISH_LIST_V1 = "sync-wishlist-v1"

    override def transform(tp: EVENT, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
      copyTo.copy(
        eventId = row.getAsOption("eventId"),
        eventName = row.getAsOption("eventName"),
        eventValue = row.getAsOption("value")
      )
    }
  }
  implicit object UiaPropsTransformer extends PropsTransformer[VIEW] {
    override def transform(tp: VIEW, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
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
  }
  implicit object VarPropsTransformer extends PropsTransformer[VAR] {
    override def transform(tp: VAR, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
      val experimentMetadataRow: Row = row.getAs("experimentMetadata")
      copyTo.copy(
        engagementType = row.getAsOption("experimentEngagementType"),
        experienceId = experimentMetadataRow.getAsOption("experimentId"),
        versionId = experimentMetadataRow.getAsOption("versionId"),
        variationIds = experimentMetadataRow.getAsList("variations")
      )
    }
  }
  implicit object Id2CUIDPropsTransformer extends PropsTransformer[IDENTIFY] {
    override def transform(tp: IDENTIFY, row: Row, copyTo: RawEventV2Export): RawEventV2Export = copyTo
  }
}
