package com.mikerusoft.spark.scala.test

import io.lettuce.core.RedisClient
import org.slf4j.{Logger, LoggerFactory}

object TestRedis extends App {
  val log: Logger = LoggerFactory.getLogger("TestRedis")
  val redis = RedisClient.create("redis://localhost:6379/").connect().sync()
  redis.set("before_submit_start", "hello")
  val beforeSubmit: String = redis.get("before_submit_start")
  log.info(s">>>>>>>Success Before Submit -> ${beforeSubmit}")
  new MyCode().run()
  redis.set("after_submit_finish", "Bye")
  val afterSubmit: String = redis.get("after_submit_finish")
  log.info(s">>>>>>>Success After Submit -> ${afterSubmit}")
}
