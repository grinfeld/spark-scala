package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.infra

import scala.collection.JavaConverters._
import com.typesafe.config.Config

object ConfigWrapper {

  implicit class ConfigWrapper(config: Config) {
    def getStringOpt(path: String): Option[String] = if (config.hasPath(path)) Option(config.getString(path)) else None
    def getString(path: String, default: String): String = if (config.hasPath(path)) config.getString(path) else default
    def getIntOpt(path: String): Option[Int] = if (config.hasPath(path)) Option(config.getInt(path)) else None
    def getLongOpt(path: String): Option[Long] = if (config.hasPath(path)) Option(config.getLong(path)) else None
    def getScalaIntList(path: String): List[Int] = if (config.hasPath(path)) config.getIntList(path).asScala.map(_.toInt).toList else List()
  }

}
