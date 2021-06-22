package com.dy.spark.scala
package infra

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait PathProvider {

  protected def buildS3Paths(date: LocalDateTime, finish: LocalDateTime, agg: List[String]): List[String] = {
    if (date.compareTo(finish) >= 0) return agg
    buildS3Paths(date.plusHours(1), finish, format(date) :: agg)
  }
  protected def format(date: LocalDateTime): String
  def getPaths(start: LocalDateTime, finish: LocalDateTime): List[String]
}

trait DateHourFormatter {
  def dateFormat(date: LocalDateTime): String
  def hourFormat(date: LocalDateTime): String
}

class DefaultDateHourFormatter extends DateHourFormatter {
  private val DATE_ONLY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val HOUR_ONLY_FORMAT = DateTimeFormatter.ofPattern("HH")

  override def dateFormat(date: LocalDateTime): String = date.format(DATE_ONLY_FORMAT)

  override def hourFormat(date: LocalDateTime): String = date.format(HOUR_ONLY_FORMAT)
}

// simple formatter based on default String format - akka: .../date=%s/hour=%s/...
abstract class SimplePathProvider(val s3PathTemplate: String, val dateHourFormatter: DateHourFormatter = new DefaultDateHourFormatter) extends PathProvider {
  def format(date: LocalDateTime): String =
    s3PathTemplate.format(dateHourFormatter.dateFormat(date), dateHourFormatter.hourFormat(date))
}
