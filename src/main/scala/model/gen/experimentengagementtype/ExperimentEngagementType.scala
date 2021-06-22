package com.dy.spark.scala
package model.gen.experimentengagementtype

sealed trait ExperimentEngagementType {
  def name: String
  override def toString: String = name
}

case class IMPRESSION() extends ExperimentEngagementType {
  override def name: String = "IMPRESSION"
}
case class RIMP() extends ExperimentEngagementType {
  override def name: String = "RIMP"
}

object ExperimentEngagementType {
    def apply(name: String): ExperimentEngagementType = {
        name match {
                case "IMPRESSION" => IMPRESSION()
                case "RIMP" => RIMP()
        }
    }
}