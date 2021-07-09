package com.mikerusoft.spark.scala.apps.helpers.db

import com.typesafe.config.Config
import com.mikerusoft.spark.scala.infra.ConfigWrapper.ConfigWrapper
import java.util.Properties
import scala.io.Source
import scala.util.{Failure, Success, Try}

case class DbProps (driverPath: String, host: String, user: Option[String], password: Option[String], partitionColumn: Option[String], lowerBound: Option[Int], upperBound: Option[Int], numPartitions: Option[Int]) {
  def props(): Properties = {
    val properties = new Properties()
    properties.put("user", user.getOrElse())
    properties.put("password", password.getOrElse())
    properties
  }
}

object DbProps {
  private val JDBC_MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
  def apply(config: Config): DbProps = {
    val dbConfigPath = config.getString("mikerusoft.spark.db.config.path")
    val dbConfigMap =
      Try(Source.fromFile(dbConfigPath)) match {
        case Failure(e) => throw e
        case Success(file) => file.getLines().filter(s => !s.isBlank).map(s => s.trim)
          .map(s => s.split(" ")).map(ar => (ar(0), ar(1))).toMap
      }

    new DbProps(
      config.getString("mikerusoft.spark.db.driver", JDBC_MYSQL_DRIVER),
      dbConfigMap("host"), dbConfigMap.get("user"), dbConfigMap.get("password"),
      config.getStringOpt("mikerusoft.spark.db.partitionColumn"), // customerId
      config.getIntOpt("mikerusoft.spark.db.lowerBound"),
      config.getIntOpt("mikerusoft.spark.db.upperBound"),
      config.getIntOpt("mikerusoft.spark.db.numPartitions"),
    )
  }
}