package com.dy.spark.scala
package model.gen

case class RcomData(widgetId: Int, feedId: Int, contextData: Option[PageContext], slotsData: Option[List[RcomSlotData]])