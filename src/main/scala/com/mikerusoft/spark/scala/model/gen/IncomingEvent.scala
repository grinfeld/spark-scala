package com.mikerusoft.spark.scala
package model.gen

case class IncomingEvent(customerId: Int, userId: Long, requestTimestamp: Long, procTimestamp: Long,
                         eventType: String, clientData: Option[ClientData], EventProps: Option[EventProps],
                         identifyProps: Option[IdentifyProps], viewProps: Option[ViewProps], varProps: Option[VarProps], rri: Int)