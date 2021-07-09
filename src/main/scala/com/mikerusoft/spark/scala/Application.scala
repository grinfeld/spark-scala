package com.mikerusoft.spark.scala

import com.mikerusoft.spark.scala.apps.dedup.{DedupApp, DedupArgs}
import com.typesafe.config.{Config, ConfigFactory}

object Application extends App{

  val config: Config = ConfigFactory.load()

  if (config.hasPath("mikerusoft.spark.app.name"))
    throw new IllegalArgumentException("mikerusoft.spark.app.name should be defined")

  val appName: String = config.getString("mikerusoft.spark.app.name")

  appName match {
    case "dedup" => DedupApp(DedupArgs(config))
    //case "export" =>
    case v => throw new IllegalArgumentException(s"$v appName is not supported")
  }

}
