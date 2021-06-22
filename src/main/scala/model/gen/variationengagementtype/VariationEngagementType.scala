package com.dy.spark.scala
package model.gen.variationengagementtype

sealed trait VariationEngagementType {
  def name: String
  override def toString: String = name
}

case class DECISION() extends VariationEngagementType{
  override def name: String = "DECISION"
}
case class IMPRESSION() extends VariationEngagementType{
  override def name: String = "IMPRESSION"
}
case class RIMP() extends VariationEngagementType{
  override def name: String = "RIMP"
}
case class CLICK() extends VariationEngagementType{
  override def name: String = "CLICK"
}
case class SENT() extends VariationEngagementType{
  override def name: String = "SENT"
}

object VariationEngagementType {
    def apply(name: String): VariationEngagementType = {
        name match {
                case "DECISION" => DECISION()
                case "IMPRESSION" => IMPRESSION()
                case "RIMP" => RIMP()
                case "CLICK" => CLICK()
                case "SENT" => SENT()
        }
    }
}