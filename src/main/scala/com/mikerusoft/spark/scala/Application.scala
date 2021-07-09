package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala

import apps.dedup.{DedupApp, DedupArgs}

import com.typesafe.config.{Config, ConfigFactory}

object Application extends App{

  val config: Config = ConfigFactory.load()

  if (config.hasPath("dy.spark.app.name"))
    throw new IllegalArgumentException("dy.spark.app.name should be defined")

  val appName: String = config.getString("dy.spark.app.name")

  appName match {
    case "dedup" => DedupApp(DedupArgs(config))
    //case "export" =>
    case v => throw new IllegalArgumentException(s"$v appName is not supported")
  }

}
