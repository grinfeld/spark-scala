package com.mikerusoft.spark.scala.test

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class MyCode() extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger("MyCode")
  @transient lazy val redis = new RedisFactory()

  def run():Unit = {
    val sparkSession = SparkSession.builder.appName("test").master("local[*]")
      .config("spark.executor.cores", "4")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List(1,2,3))


    dataset.
      foreachPartition((it: Iterator[Int]) => it.foreach(i => {
        redis.redisCommands().set(i.toString, s"${(i * 5).toString}")
        val res: String = redis.redisCommands().get(i.toString)
        log.info(s"<<<<<<Success -> ${res}")
      }))

    redis.redisCommands().set("driver", "hello")
    val res: String = redis.redisCommands().get("driver")
    log.info(s">>>>>>>Success Driver -> ${res}")
  }
}
