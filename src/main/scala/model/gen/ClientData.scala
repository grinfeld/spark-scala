package com.dy.spark.scala
package model.gen

case class ClientData(userAgent: Option[String], geoLocation: Option[GeoLocation], weather: Option[String], requestReferrer: Option[String], sessionReferrer: Option[String], sessionTrafficSource: Option[String], audiences: List[Int], colours: List[Int], pageUrl: Option[String], clientAttributionState: List[ExperimentAttribution])