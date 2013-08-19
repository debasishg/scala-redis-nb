package com.redis.protocol

import RedisCommand._
import com.redis.serialization.{Read, Write}


object NodeCommands {

  case class Save(bg: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk(List((if (bg) "BGSAVE" else "SAVE")))
  }

  case object LastSave extends RedisCommand[Long] {
    def line = multiBulk(List("LASTSAVE"))
  }

  case object Shutdown extends RedisCommand[Boolean] {
    def line = multiBulk(List("SHUTDOWN"))
  }

  case object BGRewriteAOF extends RedisCommand[Boolean] {
    def line = multiBulk(List("BGREWRITEAOF"))
  }

  case class Info(section: String) extends RedisCommand[Option[String]] {
    def line = multiBulk(List("INFO", section))
  }

  case object Monitor extends RedisCommand[Boolean] {
    def line = multiBulk(List("MONITOR"))
  }

  case class SlaveOf(node: Option[(String, Int)]) extends RedisCommand[Boolean] {
    def line = multiBulk(
      node match {
        case Some((h: String, p: Int)) => "SLAVEOF" :: List(h, p.toString)
        case _ => List("SLAVEOF", "NO", "ONE")
      }
    )
  }

  case object ClientGetName extends RedisCommand[Option[String]] {
    def line = multiBulk(List("CLIENT", "GETNAME"))
  }

  case class ClientSetName(name: String) extends RedisCommand[Boolean] {
    def line = multiBulk(List("CLIENT", "SETNAME", name))
  }

  case class ClientKill(ipPort: String) extends RedisCommand[Boolean] {
    def line = multiBulk(List("CLIENT", "KILL", ipPort))
  }

  case object ClientList extends RedisCommand[Option[String]] {
    def line = multiBulk(List("CLIENT", "LIST"))
  }

  case class ConfigGet[A](globStyleParam: String)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk(List("CONFIG", "GET", globStyleParam))
  }

  case class ConfigSet[A](param: String, value: A)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("CONFIG" :: List("SET", param, write(value)))
  }

  case object ConfigResetStat extends RedisCommand[Boolean] {
    def line = multiBulk(List("CONFIG", "RESETSTAT"))
  }

  case object ConfigRewrite extends RedisCommand[Boolean] {
    def line = multiBulk(List("CONFIG", "REWRITE"))
  }
}
