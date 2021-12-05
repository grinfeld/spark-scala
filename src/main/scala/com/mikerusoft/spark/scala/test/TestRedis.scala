package com.mikerusoft.spark.scala.test

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object TestRedis extends App {

  @transient lazy val redis = new RedisFactory()

  val sparkSession = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

  import sparkSession.implicits._

  val dataset = sparkSession.createDataset(List(1,2,3))

  dataset.
    foreachPartition((it: Iterator[Int]) => it.foreach(i => {
      redis.redisCommands().sadd("haha", i.toString)
      println("<<<<<<Success")
    }))

}
