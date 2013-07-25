package com.redis.command

import RedisCommand._
import com.redis.RedisReply
import com.redis.serialization.Format


object NodeCommands {

  case class Save(bg: Boolean = false) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq((if (bg) "BGSAVE" else "SAVE")))
    val ret  = (_: RedisReply).asBoolean
  }

  case object LastSave extends NodeCommand {
    type Ret = Long
    val line = multiBulk(Seq("LASTSAVE"))
    val ret  = (_: RedisReply).asLong
  }

  case object Shutdown extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("SHUTDOWN"))
    val ret  = (_: RedisReply).asBoolean
  }

  case object BGRewriteAOF extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("BGREWRITEAOF"))
    val ret  = (_: RedisReply).asBoolean
  }

  case class Info(section: String) extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("INFO", section))
    val ret  = (_: RedisReply).asBulk
  }

  case object Monitor extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("MONITOR"))
    val ret  = (_: RedisReply).asBoolean
  }

  case class SlaveOf(options: Any)(implicit format: Format) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(
      options match {
        case (h: String, p: Int) => "SLAVEOF" +: (Seq(h, p) map format.apply)
        case _ => Seq("SLAVEOF", "NO", "ONE")
      }
    )
    val ret  = (_: RedisReply).asBoolean
  }

  case object ClientGetName extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CLIENT", "GETNAME"))
    val ret  = (_: RedisReply).asBulk
  }

  case class ClientSetName(name: String) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CLIENT", "SETNAME", name))
    val ret  = (_: RedisReply).asBoolean
  }

  case class ClientKill(ipPort: String) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CLIENT", "KILL", ipPort))
    val ret  = (_: RedisReply).asBoolean
  }

  case object ClientList extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CLIENT", "LIST"))
    val ret  = (_: RedisReply).asBulk
  }

  case class ConfigGet(globStyleParam: String) extends NodeCommand {
    type Ret = Option[String]
    val line = multiBulk(Seq("CONFIG", "GET", globStyleParam))
    val ret  = (_: RedisReply).asBulk
  }

  case class ConfigSet(param: String, value: Any)(implicit format: Format) extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk("CONFIG" +: (Seq("SET", param, value) map format.apply))
    val ret  = (_: RedisReply).asBoolean
  }

  case object ConfigResetStat extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CONFIG", "RESETSTAT"))
    val ret  = (_: RedisReply).asBoolean
  }

  case object ConfigRewrite extends NodeCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("CONFIG", "REWRITE"))
    val ret  = (_: RedisReply).asBoolean
  }
}
