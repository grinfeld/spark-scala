package com.mikerusoft.spark.scala.test

import io.lettuce.core.RedisClient
import io.lettuce.core.api.sync.RedisCommands
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class MyCode() extends Serializable {
  @transient lazy val log: Logger = LoggerFactory.getLogger("MyCode")
  //@transient lazy val redis = new RedisFactory()
  @transient lazy val redis: RedisCommands[String, String] = RedisClient.create("redis://localhost:6379/").connect().sync()

  def run():Unit = {
    val sparkSession = SparkSession.builder.appName("test").master("local[*]")
      .config("spark.executor.cores", "4")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List(Pojo(1),Pojo(2),Pojo(3)))

    dataset
      .foreachPartition((it: Iterator[Pojo]) => it.foreach(p => {
        //val redis = RedisClient.create("redis://localhost:6379/").connect().sync()
        redis.set(p.i.toString, s"${(p.i * 5).toString}")
        val res: String = redis.get(p.i.toString)
        log.info(s"<<<<<<Success -> ${res}")
      }))
  }
}
