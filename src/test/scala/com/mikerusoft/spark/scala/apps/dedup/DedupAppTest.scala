package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.apps.dedup


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
    val clientData = ClientData(None, None, Option("Google"), Option("Direct"), List())
    val props = EventProps("purchase", Option(10), Option("purchase-v1"), Option("USD"), Option(1000))
    val expected = IncomingEvent(
      123456, 12345678, 1624196839945L, 1624196839945L, "EVENT",
      Option(clientData), Option(props), None, None, None, 123456
    )

/*    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    sparkSession.createDataFrame(List(expected))
      .write.parquet(sparkTmpDir + "raw")*/

    /*val encoderSchema = Encoders.product[RawEventV2].schema


    sparkSession.read.parquet(sparkTmpDir + "raw").as(Encoders.product[RawEventV2])
      .foreach(f => println(f))*/

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
      .as(Encoders.product[IncomingEvent]).first()
    assertResult(expected)(actual)

  }

  protected override def afterEach(): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
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
