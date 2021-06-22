package com.dy.spark.scala
package model.gen

case class DpxProps(eventName: String, eventId: Option[Int], dyType: Option[String], reportedCurrency: Option[String], reportedValue: Option[Double], value: Option[Long], eventProperties: Option[String], goalInfos: List[GoalInfo])