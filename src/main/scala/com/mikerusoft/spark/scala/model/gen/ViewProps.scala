package com.mikerusoft.spark.scala
package model.gen

case class ViewProps(siteVariables: Map[String,String], pageContext: Option[PageContext], mobileData: Option[MobileData])