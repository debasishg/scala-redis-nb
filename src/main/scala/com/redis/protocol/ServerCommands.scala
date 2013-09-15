package com.redis.protocol

import com.redis.serialization._


object ServerCommands {
  import DefaultWriters._

  case object Save extends RedisCommand[Boolean]("SAVE") {
    def params = ANil
  }

  case object BgSave extends RedisCommand[Boolean]("BGSAVE") {
    def params = ANil
  }

  case object LastSave extends RedisCommand[Long]("LASTSAVE") {
    def params = ANil
  }

  case object Shutdown extends RedisCommand[Boolean]("SHUTDOWN") {
    def params = ANil
  }

  case object BGRewriteAOF extends RedisCommand[Boolean]("BGREWRITEAOF") {
    def params = ANil
  }

  case object Info extends RedisCommand[Option[String]]("INFO") {
    def params = ANil
  }

  case object Monitor extends RedisCommand[Boolean]("MONITOR") {
    def params = ANil
  }

  case class SlaveOf(node: Option[(String, Int)]) extends RedisCommand[Boolean]("SLAVEOF") {
    def params = node match {
      case Some((h: String, p: Int)) => h +: p +: ANil
      case _ => "NO" +: "ONE" +: ANil
    }
  }


  object Client {

    case object GetName extends RedisCommand[Option[String]]("CLIENT") {
      def params = "GETNAME" +: ANil
    }

    case class SetName(name: String) extends RedisCommand[Boolean]("CLIENT") {
      def params = "SETNAME" +: ANil
    }

    case class Kill(ipPort: String) extends RedisCommand[Boolean]("CLIENT") {
      def params = "KILL" +: ipPort +: ANil
    }

    case object List extends RedisCommand[Option[String]]("CLIENT") {
      def params = "LIST" +: ANil
    }

  }


  object Config {

    case class Get[A: Reader](globStyleParam: String) extends RedisCommand[Option[A]]("CONFIG") {
      def params = "GET" +: globStyleParam +: ANil
    }

    case class Set(param: String, value: Stringified) extends RedisCommand[Boolean]("CONFIG") {
      def params = "SET" +: param +: value +: ANil
    }

    case object ResetStat extends RedisCommand[Boolean]("CONFIG") {
      def params = "RESETSTAT" +: ANil
    }

    case object Rewrite extends RedisCommand[Boolean]("CONFIG") {
      def params = "REWRITE" +: ANil
    }

  }
}
