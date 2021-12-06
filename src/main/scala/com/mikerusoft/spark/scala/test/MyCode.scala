package com.mikerusoft.spark.scala.test

import io.lettuce.core.RedisClient
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class MyCode() extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger("MyCode")
  def run():Unit = {
    val sparkSession = SparkSession.builder.appName("test").master("local[*]")
      .config("spark.executor.cores", "4")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List(1,2,3))

    val redisClient = RedisClient.create("redis://localhost:6379/")
    val redis = redisClient.connect.sync()

    dataset.
      foreachPartition((it: Iterator[Int]) => it.foreach(i => {
        redis.set(i.toString, s"${(i * 5).toString}")
        val res: String = redis.get(i.toString)
        log.info(s"<<<<<<Success -> ${res}")
      }))

    redis.set("driver", "hello")
    val res: String = redis.get("driver")
    log.info(s">>>>>>>Success Driver -> ${res}")
  }
}
