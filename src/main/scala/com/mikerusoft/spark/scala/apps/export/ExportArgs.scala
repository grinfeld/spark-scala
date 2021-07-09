package com.mikerusoft.spark.scala.apps.`export`

import com.amazonaws.regions.Regions
import com.mikerusoft.spark.scala.apps.SimpleArgs
import com.typesafe.config.Config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class ExportArgs private[`export`] (override val runDate: LocalDateTime, override val showExecutionPlan: Boolean,
                  override val appName: String, override val master: Option[String],
                  inputPath: String, outputPath: String, copyOutputPath: String, onlyDate: String,
                  dbConfigPath: String, offsetHour: Int, specificSection: Option[Int], coalesceValue: Option[Int],
                  region: Regions, actualDate: LocalDateTime, enableTransfer: Boolean
  ) extends SimpleArgs (runDate, showExecutionPlan, appName, master) {

}

object ExportArgs {
  private val DATE_ONLY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val DEF_INPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")

  def apply(config: Config): ExportArgs = {
    val simple = SimpleArgs(config, DEF_INPUT_FORMAT)
    val actualDate = simple.runDate.minusHours(1)
    new ExportArgs(
      simple.runDate,
      simple.showExecutionPlan,
      simple.appName,
      simple.master,
      config.getString("mikerusoft.spark.input.path"),
      config.getString("mikerusoft.spark.output.path"),
      config.getString("mikerusoft.spark.output.copyto.path"),
      simple.runDate.minusHours(1).format(DATE_ONLY_FORMAT),
      config.getString("mikerusoft.spark.dbconfig.path"),
      getClosestOffset(actualDate),
      if (config.hasPath("mikerusoft.spark.sections")) Option(config.getInt("mikerusoft.spark.sections")) else None,
      if (config.hasPath("mikerusoft.spark.coalesce")) Option(config.getInt("mikerusoft.spark.coalesce")) else None,
      if (config.hasPath("mikerusoft.spark.region") && config.getString("mikerusoft.spark.region").contains("euc")) Regions.EU_CENTRAL_1 else Regions.US_EAST_1,
      actualDate,
      config.hasPath("mikerusoft.spark.enable.copy") && config.getBoolean("mikerusoft.spark.enable.copy")
    )
  }

  private def getClosestOffset(now: LocalDateTime): Int = if (now.getHour < 12) now.getHour * (-1) else 24 - now.getHour
}
