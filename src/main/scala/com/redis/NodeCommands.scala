package com.redis

import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.util.ByteString


object NodeCommands {

  case class Save(bg: Boolean = false) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq((if (bg) "BGSAVE" else "SAVE")))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object LastSave extends NodeCommand {
    type Ret = Long
    val line = multiBulk(Seq("LASTSAVE"))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case object Shutdown extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("SHUTDOWN"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object BGRewriteAOF extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("BGREWRITEAOF"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class Info(section: String) extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("INFO", section))
    val ret  = RedisReply(_: ByteString).asBulk
  }

  case object Monitor extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("MONITOR"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class SlaveOf(options: Any)(implicit format: Format) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(
      options match {
        case (h: String, p: Int) => "SLAVEOF" +: (Seq(h, p) map format.apply)
        case _ => Seq("SLAVEOF", "NO", "ONE")
      }
    )
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object ClientGetName extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CLIENT", "GETNAME"))
    val ret  = RedisReply(_: ByteString).asBulk
  }

  case class ClientSetName(name: String) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CLIENT", "SETNAME", name))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class ClientKill(ipPort: String) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CLIENT", "KILL", ipPort))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object ClientList extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CLIENT", "LIST"))
    val ret  = RedisReply(_: ByteString).asBulk
  }

  case class ConfigGet(globStyleParam: String) extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CONFIG", "GET", globStyleParam))
    val ret  = RedisReply(_: ByteString).asBulk
  }

  case class ConfigSet(param: String, value: Any)(implicit format: Format) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk("CONFIG" +: (Seq("SET", param, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object ConfigResetStat extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CONFIG", "RESETSTAT"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object ConfigRewrite extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CONFIG", "REWRITE"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
}
