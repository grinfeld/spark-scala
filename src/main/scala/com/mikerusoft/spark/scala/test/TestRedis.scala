package com.mikerusoft.spark.scala.test

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TestRedis extends App {

  @transient lazy val redis = new RedisFactory()
  @transient lazy val log = LoggerFactory.getLogger("TestRedis")

  val sparkSession = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

  import sparkSession.implicits._

  val dataset = sparkSession.createDataset(List(1,2,3))

  dataset.
    foreachPartition((it: Iterator[Int]) => it.foreach(i => {
      redis.redisCommands().set(i.toString, s"${(i * 5).toString}")
      val res: String = redis.redisCommands().get(i.toString)
      log.info(s"<<<<<<Success -> ${res}")
    }))

}
