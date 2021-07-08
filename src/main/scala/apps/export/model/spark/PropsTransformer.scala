package com.dy.spark.scala
package apps.`export`.model.spark

import apps.`export`.model.{PRODUCT, RawEventV2Export, RecommendationContextType}
import infra.spark.RowWrapper.RowWrapper
import model.gen.raweventtype._

import org.apache.spark.sql.Row

trait PropsTransformer[T] {
  def transform(tp: T, row: Row, copyTo: RawEventV2Export): RawEventV2Export
}

object PropsTransformer {

  implicit class EventTypeTransformer[T <: RawEventType](tp: T) {
    def transform(row: Row, copyTo: RawEventV2Export)(implicit transformer: PropsTransformer[T]) : RawEventV2Export =
      transformer.transform(tp, row, copyTo)
  }

  implicit object DpxPropsTransformer extends PropsTransformer[DPX] {
    val REMOVE_FROM_CART_V1 = "remove-from-cart-v1"
    val ADD_TO_WISHLIST_V1 = "add-to-wishlist-v1"
    val ADD_TO_CART_V1 = "add-to-cart-v1"
    val PURCHASE_V1 = "purchase-v1"
    val SYNC_CART_V1 = "sync-cart-v1"
    val SYNC_WISH_LIST_V1 = "sync-wishlist-v1"

    override def transform(tp: DPX, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
      copyTo.copy(
        eventId = row.getAsOption("eventId"),
        eventName = row.getAsOption("eventName"),
        eventProperties = row.getAsOption("eventProperties"),
        eventValue = row.getAsOption("value")
      )
    }
  }
  implicit object UiaPropsTransformer extends PropsTransformer[UIA] {
    override def transform(tp: UIA, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
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
  implicit object VarPropsTransformer extends PropsTransformer[VARIATION_ENGAGEMENT] {
    override def transform(tp: VARIATION_ENGAGEMENT, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
      val experimentMetadataRow: Row = row.getAs("experimentMetadata")
      copyTo.copy(
        engagementType = row.getAsOption("experimentEngagementType"),
        experienceId = experimentMetadataRow.getAsOption("experimentId"),
        versionId = experimentMetadataRow.getAsOption("versionId"),
        variationIds = experimentMetadataRow.getAsList("variations")
      )
    }
  }
  // deprecated
  implicit object ExpEngPropsTransformer extends PropsTransformer[EXPERIMENT_ENGAGEMENT] {
    override def transform(tp: EXPERIMENT_ENGAGEMENT, row: Row, copyTo: RawEventV2Export): RawEventV2Export = {
      val experimentMetadataRow: Row = row.getAs("experimentMetadata")
      copyTo.copy(
        engagementType = row.getAsOption("experimentEngagementType"),
        experienceId = experimentMetadataRow.getAsOption("experimentId"),
        versionId = experimentMetadataRow.getAsOption("versionId"),
        variationIds = experimentMetadataRow.getAsList("variations")
      )
    }
  }
  implicit object UnitClickPropsTransformer extends PropsTransformer[UNIT_CLICK] {
    override def transform(tp: UNIT_CLICK, row: Row, copyTo: RawEventV2Export): RawEventV2Export = copyTo
  }
  implicit object Id2CUIDPropsTransformer extends PropsTransformer[ID_CUID] {
    override def transform(tp: ID_CUID, row: Row, copyTo: RawEventV2Export): RawEventV2Export = copyTo
  }
  // deprecated
  implicit object SmartLinkPropsTransformer extends PropsTransformer[SMARTLINK_CLICK] {
    override def transform(tp: SMARTLINK_CLICK, row: Row, copyTo: RawEventV2Export): RawEventV2Export = copyTo
  }
  // deprecated
  implicit object UnitImpPropsTransformer extends PropsTransformer[UNIT_IMP] {
    override def transform(tp: UNIT_IMP, row: Row, copyTo: RawEventV2Export): RawEventV2Export = copyTo
  }
}
