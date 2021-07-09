package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.model.gen

case class ExperimentAttribution(experimentMetadata: ExperimentMetadata, expVisitId: Long, chooseVariationMechanism: Option[Int], secondsAfterAttributionTrigger: Option[Long])