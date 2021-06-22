package com.dy.spark.scala
package model.gen

case class UiaProps(siteVariables: Map[String,String], pageContext: Option[PageContext], goalInfos: List[GoalInfo], mobileData: Option[MobileData])