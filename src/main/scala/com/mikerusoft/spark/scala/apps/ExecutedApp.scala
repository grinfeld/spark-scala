package com.mikerusoft.spark.scala.apps

import org.apache.spark.sql.SparkSession

abstract class ExecutedApp[T <: SimpleArgs](val args: T) {
  def start():Unit

  protected def createSparkSessionBuilder(): SparkSession.Builder = {
    args.master match {
      case Some(master) => SparkSession.builder.appName(args.appName).master(master)
      case None => SparkSession.builder.appName(args.appName)
    }
  }
}
