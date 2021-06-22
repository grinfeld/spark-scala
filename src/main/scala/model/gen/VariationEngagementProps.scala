package com.dy.spark.scala
package model.gen

case class VariationEngagementProps(variationEngagementType: String, experimentMetadata: ExperimentMetadata, expVisitId: Long, chooseVariationMechanism: Option[Int], rcomData: Option[RcomData])