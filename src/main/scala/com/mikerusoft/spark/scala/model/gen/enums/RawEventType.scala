package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.model.gen.enums

sealed trait RawEventType {
  def name: String
  override def toString: String = name
}

case class VIEW() extends RawEventType {
  override def name: String = "VIEW"
}
case class EVENT() extends RawEventType{
  override def name: String = "EVENT"
}
case class IDENTIFY() extends RawEventType{
  override def name: String = "IDENTIFY"
}
case class VAR() extends RawEventType{
  override def name: String = "VAR"
}
case class NA() extends RawEventType {
  override def name: String = "NA"
}

object RawEventType {
    private val NA = new NA()

    private val VALUES = Map(
      ("VIEW", VIEW()),
      ("EVENT", EVENT()),
      ("IDENTIFY", IDENTIFY()),
      ("VAR", VAR()),
      ("NA", NA)
    )

    def apply(name: String): RawEventType = {
      VALUES.getOrElse(name.toUpperCase, NA)
    }
}