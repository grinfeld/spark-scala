package com.dy.spark.scala
package apps.`export`.model

sealed trait RecommendationContextType {
  def typeOrdinal: Int
  def name: String
}

case class ALL_CONTEXTS() extends RecommendationContextType {
  override def typeOrdinal: Int = 0
  override def name: String = "ALL_CONTEXTS"
}
case class HOMEPAGE() extends RecommendationContextType {
  override def typeOrdinal: Int = 1
  override def name: String = "HOMEPAGE"
}
case class CATEGORY() extends RecommendationContextType {
  override def typeOrdinal: Int = 2
  override def name: String = "CATEGORY"
}
case class PRODUCT() extends RecommendationContextType {
  override def typeOrdinal: Int = 3
  override def name: String = "PRODUCT"
}
case class POST() extends RecommendationContextType {
  override def typeOrdinal: Int = 4
  override def name: String = "POST"
}
case class CART() extends RecommendationContextType {
  override def typeOrdinal: Int = 5
  override def name: String = "CART"
}
case class EMAIL() extends RecommendationContextType {
  override def typeOrdinal: Int = 6
  override def name: String = "EMAIL"
}
case class SEARCH() extends RecommendationContextType {
  override def typeOrdinal: Int = 7
  override def name: String = "SEARCH"
}
case class OTHER() extends RecommendationContextType {
  override def typeOrdinal: Int = 8
  override def name: String = "OTHER"
}

object RecommendationContextType {

  private val other = OTHER();
  
  private val ORDINALS = Map(
    (0, ALL_CONTEXTS()),
    (1, HOMEPAGE()),
    (2, CATEGORY()),
    (3, PRODUCT()),
    (4, POST()),
    (5, CART()),
    (6, EMAIL()),
    (7, SEARCH()),
    (8, other)
  )
  
  def fromString(value: String): RecommendationContextType = value match {
      case "ALL_CONTEXTS" => ORDINALS.getOrElse(0, other)
      case "HOMEPAGE" => ORDINALS.getOrElse(1, other)
      case "CATEGORY" => ORDINALS.getOrElse(2, other)
      case "PRODUCT" => ORDINALS.getOrElse(3, other)
      case "POST" => ORDINALS.getOrElse(4, other)
      case "CART" => ORDINALS.getOrElse(5, other)
      case "EMAIL" => ORDINALS.getOrElse(6, other)
      case "SEARCH" => ORDINALS.getOrElse(7, other)
      case _ => other
    }

  def fromOptString(str: Option[String]): RecommendationContextType =
    str match {
      case None => other
      case Some(value) => RecommendationContextType.fromString(value)
    }

  def apply(ordinal: Option[Int]): RecommendationContextType = ordinal match {
    case None => other
    case Some(value) => ORDINALS.getOrElse(value, other)
  }

}
