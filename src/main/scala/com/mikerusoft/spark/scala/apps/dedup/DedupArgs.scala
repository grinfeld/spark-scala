package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.dedup

import infra.ConfigWrapper.ConfigWrapper
import com.typesafe.config.Config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class DedupArgs private[dedup] (override val runDate: LocalDateTime, override val showExecutionPlan: Boolean,
                                                override val appName: String, override val master: Option[String],
                 inputFormat: Option[String], inputPath: String, outputPath: String, onlyDate: String, onlyHour: String, specificSections: List[Int])
    extends SimpleArgs (runDate, showExecutionPlan, appName, master) with WithSectionListArgs

object DedupArgs {

  private val DATE_ONLY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val HOUR_ONLY_FORMAT = DateTimeFormatter.ofPattern("HH")
  private val DEF_INPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")

  def apply(config: Config): DedupArgs = {
    val simple = SimpleArgs(config, DEF_INPUT_FORMAT)

    new DedupArgs(
      simple.runDate,
      simple.showExecutionPlan,
      simple.appName,
      simple.master,
      config.getStringOpt("dy.spark.input.format"),
      config.getString("dy.spark.input.path"),
      config.getString("dy.spark.output.path"),
      simple.runDate.minusHours(1).format(DATE_ONLY_FORMAT),
      simple.runDate.minusHours(1).format(HOUR_ONLY_FORMAT),
      config.getScalaIntList("dy.spark.sections")
    )
  }
}