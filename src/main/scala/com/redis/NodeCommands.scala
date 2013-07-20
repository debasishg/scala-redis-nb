package com.redis

import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._

object NodeCommands {

  case class Save(bg: Boolean = false) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq((if (bg) "BGSAVE" else "SAVE").getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object LastSave extends NodeCommand {
    type Ret = Long
    val line = multiBulk(Seq("LASTSAVE".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case object Shutdown extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("SHUTDOWN".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object BGRewriteAOF extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("BGREWRITEAOF".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class Info(section: String) extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("INFO", section).map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBulk
  }

  case object Monitor extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("MONITOR".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class SlaveOf(options: Any)(implicit format: Format) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(
      options match {
        case (h: String, p: Int) => "SLAVEOF".getBytes("UTF-8") +: (Seq(h, p) map format.apply)
        case _ => Seq("SLAVEOF", "NO", "ONE").map(_.getBytes("UTF-8"))
      }
    )
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object ClientGetName extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CLIENT", "GETNAME").map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBulk
  }

  case class ClientSetName(name: String) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CLIENT", "SETNAME", name).map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class ClientKill(ipPort: String) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CLIENT", "KILL", ipPort).map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object ClientList extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CLIENT", "LIST").map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBulk
  }

  case class ConfigGet(globStyleParam: String) extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CONFIG", "GET", globStyleParam).map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBulk
  }

  case class ConfigSet(param: String, value: Any)(implicit format: Format) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk("CONFIG".getBytes("UTF-8") +: (Seq("SET", param, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object ConfigResetStat extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CONFIG", "RESETSTAT").map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object ConfigRewrite extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CONFIG", "REWRITE").map(_.getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }
}
