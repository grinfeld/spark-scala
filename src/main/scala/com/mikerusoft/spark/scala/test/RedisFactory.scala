package com.mikerusoft.spark.scala.test

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands

class RedisFactory {

  private lazy val syncCommands: RedisCommands[String, String] = init()

  private def init(): RedisCommands[String, String] = {
    val redisClient: RedisClient = RedisClient.create("redis://localhost:6379/")
    val connection: StatefulRedisConnection[String, String] = redisClient.connect
    connection.sync
  }

  def redisCommands(): RedisCommands[String, String] = syncCommands
}
