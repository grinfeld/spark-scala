package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.model.gen.enums

sealed trait VarType {
  def name: String
  override def toString: String = name
}

case class IMP() extends VarType{
  override def name: String = "IMPRESSION"
}
case class CLICK() extends VarType{
  override def name: String = "CLICK"
}
case class SENT() extends VarType{
  override def name: String = "SENT"
}
case class NAVAR() extends VarType{
  override def name: String = "NA"
}

object VarType {
  private val NA = NAVAR()
  private val VALUES = Map(
    ("IMPRESSION", IMP()),
    ("CLICK", CLICK()),
    ("SENT", SENT())
  )

  def apply(name: String): VarType = {
    VALUES.getOrElse(name.toUpperCase, NA)
  }
}