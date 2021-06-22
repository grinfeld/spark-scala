package com.dy.spark.scala
package model.gen

case class ExperimentEngagementProps(experimentEngagementType: String, experimentMetadata: ExperimentMetadata, expVisitId: Long, chooseVariationMechanism: Option[Int], rcomData: Option[RcomData])