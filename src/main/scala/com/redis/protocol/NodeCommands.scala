package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object NodeCommands {

  case class Save(bg: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk(Seq((if (bg) "BGSAVE" else "SAVE")))
  }

  case object LastSave extends RedisCommand[Long] {
    def line = multiBulk(Seq("LASTSAVE"))
  }

  case object Shutdown extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("SHUTDOWN"))
  }

  case object BGRewriteAOF extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("BGREWRITEAOF"))
  }

  case class Info(section: String) extends RedisCommand[Option[String]] {
    def line = multiBulk(Seq("INFO", section))
  }

  case object Monitor extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("MONITOR"))
  }

  case class SlaveOf(node: Option[(String, Int)]) extends RedisCommand[Boolean] {
    def line = multiBulk(
      node match {
        case Some((h: String, p: Int)) => "SLAVEOF" +: Seq(h, p.toString)
        case _ => Seq("SLAVEOF", "NO", "ONE")
      }
    )
  }


  object Client {

    case object GetName extends RedisCommand[Option[String]] {
      def line = multiBulk(Seq("CLIENT", "GETNAME"))
    }

    case class SetName(name: String) extends RedisCommand[Boolean] {
      def line = multiBulk(Seq("CLIENT", "SETNAME", name))
    }

    case class Kill(ipPort: String) extends RedisCommand[Boolean] {
      def line = multiBulk(Seq("CLIENT", "KILL", ipPort))
    }

    case object List extends RedisCommand[Option[String]] {
      def line = multiBulk(Seq("CLIENT", "LIST"))
    }

  }


  object Config {

    case class Get[A](globStyleParam: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
      def line = multiBulk(Seq("CONFIG", "GET", globStyleParam))
    }

    case class Set(param: String, value: Stringified) extends RedisCommand[Boolean] {
      def line = multiBulk("CONFIG" +: Seq("SET", param, value.toString))
    }

    case object ResetStat extends RedisCommand[Boolean] {
      def line = multiBulk(Seq("CONFIG", "RESETSTAT"))
    }

    case object Rewrite extends RedisCommand[Boolean] {
      def line = multiBulk(Seq("CONFIG", "REWRITE"))
    }

  }
}
