package com.mikerusoft.spark.scala.test

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands

class RedisFactory(i: Int) {

  def this() = {
    this(0)
    println("fdgdfgd")
  }

  private lazy val redisClient: RedisClient = init()

  private def init(): RedisClient = {
    RedisClient.create("redis://localhost:6379/")
  }

  def redisCommands(): RedisCommands[String, String] = {
    val connection: StatefulRedisConnection[String, String] = redisClient.connect
    connection.sync
  }
}
