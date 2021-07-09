package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.model.gen

case class ClientData(userAgent: Option[String], geoLocation: Option[GeoLocation], sessionReferrer: Option[String], sessionTrafficSource: Option[String], audiences: List[Int])