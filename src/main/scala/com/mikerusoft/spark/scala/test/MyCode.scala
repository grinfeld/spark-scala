package com.mikerusoft.spark.scala.test

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class MyCode() {
  @transient lazy val log: Logger = LoggerFactory.getLogger("MyCode")
  // this code evaluated
  @transient lazy val redis = new RedisFactory()

  def run():Unit = {
    val sparkSession = SparkSession.builder.appName("test").master("local[*]")
      .config("spark.executor.cores", "4")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List(Pojo(1),Pojo(2),Pojo(3)))

    dataset
      .filter(p => {
        redis.redisCommands().ping()
        true
      })
      .map(p => p.i)
      .foreachPartition((it: Iterator[Int]) => it.foreach(i => {
        //val redis = RedisClient.create("redis://localhost:6379/").connect().sync()
        redis.redisCommands().set(i.toString, s"${(i * 5).toString}")
        val res: String = redis.redisCommands().get(i.toString)
        log.info(s"<<<<<<Success -> ${res}")
      }))
  }
}
