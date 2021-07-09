package com.mikerusoft.spark.scala
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
      config.getString("dy.spark.run.datehour"),
      if (config.hasPath("dy.spark.execution.plan")) config.getBoolean("dy.spark.execution.plan") else false,
      config.getString("dy.spark.app.name"),
      if (config.hasPath("dy.spark.run.date.format") && !config.getString("dy.spark.run.date.format").isBlank)
        DateTimeFormatter.ofPattern(config.getString("dy.spark.run.date.format"))
      else dateFormat,
      if (config.hasPath("spark.master")) Option(config.getString("spark.master")) else None
    )
  }
}

