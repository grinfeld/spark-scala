package com.dy.spark.scala
package apps.`export`.db

import apps.helpers.db.{DbDatasetFlow, DbProps}
import infra.SimplePathProvider
import infra.spark.DatasetTypes.SparkSessionType
import infra.spark.StartFlow

import org.apache.spark.sql.Encoders

import java.time.LocalDateTime


object S3ExportPathsBySectionFlow {
  private val query = "select sec.time_zone_offset from dypd_sections as sec where sec.id = "

  private val pathProvider = new SimplePathProvider("date=%s/hour=%s/") {
    override def getPaths(start: LocalDateTime, finish: LocalDateTime): List[String] = buildS3Paths(start, finish, List())
  }

  def apply(dbProps: DbProps, sectionId: Int, date: LocalDateTime): SparkSessionType[String] = {
    DbDatasetFlow(dbProps, query + s"$sectionId")
      .map(ds =>
        ds.select("time_zone_offset").as(Encoders.scalaInt)
          .map(offset => if (offset <= 0) -offset else 24 - offset)(Encoders.scalaInt)
          .flatMap(hour => pathProvider.getPaths(date.minusDays(1).withHour(hour), date.withHour(hour)))(Encoders.STRING)
      )
  }

  def apply(date: LocalDateTime): SparkSessionType[String] = {
    new StartFlow[String](sparkSession =>
      sparkSession.createDataset(pathProvider.getPaths(date.minusDays(1), date))(Encoders.STRING)
    )
  }
}
