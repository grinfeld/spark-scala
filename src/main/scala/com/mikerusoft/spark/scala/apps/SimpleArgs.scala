package com.mikerusoft.spark.scala.apps

import com.typesafe.config.Config

import java.io.Serializable
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SimpleArgs protected(val runDate: LocalDateTime, val showExecutionPlan: Boolean, val appName: String, val master: Option[String]) extends Serializable {

  def this(runDate: String, showExecutionPlan: Boolean, appName: String, runDateFormat: DateTimeFormatter, master: Option[String]) =
    this(LocalDateTime.parse(runDate, runDateFormat), showExecutionPlan, appName, master)
}

object SimpleArgs {
  def apply(config: Config, dateFormat: DateTimeFormatter): SimpleArgs = {
    new SimpleArgs(
      config.getString("mikerusoft.spark.run.datehour"),
      if (config.hasPath("mikerusoft.spark.execution.plan")) config.getBoolean("mikerusoft.spark.execution.plan") else false,
      config.getString("mikerusoft.spark.app.name"),
      if (config.hasPath("mikerusoft.spark.run.date.format") && !config.getString("mikerusoft.spark.run.date.format").isBlank)
        DateTimeFormatter.ofPattern(config.getString("mikerusoft.spark.run.date.format"))
      else dateFormat,
      if (config.hasPath("spark.master")) Option(config.getString("spark.master")) else None
    )
  }
}

