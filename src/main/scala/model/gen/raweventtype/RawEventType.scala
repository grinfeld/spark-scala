package com.dy.spark.scala
package model.gen.raweventtype

trait RawEventType {
  def name: String
  override def toString: String = name
}

case class UIA() extends RawEventType {
  override def name: String = "UIA"
}
case class DPX() extends RawEventType{
  override def name: String = "DPX"
}
case class SMARTLINK_CLICK() extends RawEventType{
  override def name: String = "SMARTLINK_CLICK"
}
case class ID_CUID() extends RawEventType{
  override def name: String = "ID_CUID"
}
case class UNIT_CLICK() extends RawEventType{
  override def name: String = "UNIT_CLICK"
}
case class UNIT_IMP() extends RawEventType{
  override def name: String = "UNIT_IMP"
}
case class EXPERIMENT_ENGAGEMENT() extends RawEventType{
  override def name: String = "EXPERIMENT_ENGAGEMENT"
}
case class VARIATION_ENGAGEMENT() extends RawEventType{
  override def name: String = "VARIATION_ENGAGEMENT"
}

object RawEventType {
    def apply(name: String): RawEventType = {
        name match {
                case "UIA" => UIA()
                case "DPX" => DPX()
                case "SMARTLINK_CLICK" => SMARTLINK_CLICK()
                case "ID_CUID" => ID_CUID()
                case "UNIT_CLICK" => UNIT_CLICK()
                case "UNIT_IMP" => UNIT_IMP()
                case "EXPERIMENT_ENGAGEMENT" => EXPERIMENT_ENGAGEMENT()
                case "VARIATION_ENGAGEMENT" => VARIATION_ENGAGEMENT()
        }
    }
}