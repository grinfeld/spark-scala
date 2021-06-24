package com.dy.spark.scala
package apps.dedup

import model.gen.{ClientData, DpxProps, RawEventV2}

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File
import scala.reflect.io.Directory

class DedupAppTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val sparkTmpDir = new java.io.File(".").getCanonicalPath + "/tmp/spark-temp/"

  private def cleanUpSparkSession(): Unit = new Directory(new File(sparkTmpDir)).deleteRecursively()

  protected override def afterAll(): Unit = {
    //cleanUpSparkSession()
  }

  protected override def beforeEach(): Unit = {
    cleanUpSparkSession()
  }

  "when single event read from parquet" should "written after dedup single event same as read" in {
    val clientData = ClientData(None, None, None, Option("D"), None, Option("Direct"), List(), List(), Option("www.dynamicyield.com"), List())
    val props = DpxProps("purchase", Option(10), Option("purchase-v1"), Option("USD"), Option(1000), Option(1000), None, List())
    val expected = RawEventV2(
      123456, 12345678, 1624196839945L, 1624196839945L, 123456, 1624196839945L, "DPX",
      Option(clientData), Option(props), None, None, None, None, None, Option(2222), Option("123444"), None, None, 123456
    )

/*    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    sparkSession.createDataFrame(List(expected))
      .write.parquet(sparkTmpDir + "raw")*/

    val config = ConfigFactory.load()
      .withValue("spark.master", ConfigValueFactory.fromAnyRef("local[*]"))
      .withValue("dy.spark.run.datehour", ConfigValueFactory.fromAnyRef("2021-06-20 15"))
      .withValue("dy.spark.execution.plan", ConfigValueFactory.fromAnyRef("true"))
      .withValue("dy.spark.app.name", ConfigValueFactory.fromAnyRef("dedup"))
      .withValue("dy.spark.input.path", ConfigValueFactory.fromAnyRef(ClassLoader.getSystemResource("file1.parquet").getFile))
      .withValue("dy.spark.input.format", ConfigValueFactory.fromAnyRef("parquet"))
      .withValue("dy.spark.output.path", ConfigValueFactory.fromAnyRef(sparkTmpDir + "output/"))

    val args = DedupArgs(config)

    DedupApp(args).start()

    val sparkSession = SparkSession.builder().getOrCreate()
    val actual = sparkSession.read.format("parquet").load(sparkTmpDir + "output/")
      .as(Encoders.product[RawEventV2]).first()
    assertResult(expected)(actual)
  }

  protected override def afterEach(): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    cleanUpSparkSession()
    sparkSession.sharedState.cacheManager.clearCache()
    sparkSession.sessionState.catalog.reset()
    sparkSession.close()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    cleanUpSparkSession()
    ConfigFactory.defaultOverrides()
    ConfigFactory.invalidateCaches()
  }

}
