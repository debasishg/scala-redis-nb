package com.redis.protocol

import com.redis.serialization._


object ConnectionCommands {
  import DefaultWriters._

 case object Quit extends RedisCommand[Boolean]("QUIT") {
    def params = ANil
  }

  case class Auth(secret: String) extends RedisCommand[Boolean]("AUTH") {
    def params = secret +: ANil
  }

  case class Select(index: Int) extends RedisCommand[Boolean]("SELECT") {
    def params = index +: ANil
  }

  case object Ping extends RedisCommand[Boolean]("PING") {
    def params = ANil
  }

  case class Echo(message: String) extends RedisCommand[String]("ECHO") {
    def params = message +: ANil
  }
  
}
