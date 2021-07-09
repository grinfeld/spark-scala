package com.mikerusoft.spark.scala
package model.gen

case class EventProps(eventName: String, eventId: Option[Int], propType: Option[String], reportedCurrency: Option[String], reportedValue: Option[Double])