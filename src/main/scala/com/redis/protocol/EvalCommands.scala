package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object EvalCommands {
  case class EvalMultiBulk[A](script: String, keys: List[Any], args: List[Any])(implicit format: Format, parse: Parse[A]) extends EvalCommand {
    type Ret = List[A]
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args) map format.apply)
    val ret  = (_: RedisReply[_]).asList[A].flatten
  }

  case class EvalBulk[A](script: String, keys: List[Any], args: List[Any])(implicit format: Format, parse: Parse[A]) extends EvalCommand {
    type Ret = Option[A]
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args) map format.apply)
    val ret  = (_: RedisReply[_]).asBulk[A]
  }

  case class EvalMultiSHA[A](script: String, keys: List[Any], args: List[Any])(implicit format: Format, parse: Parse[A]) extends EvalCommand {
    type Ret = List[A]
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args) map format.apply)
    val ret  = (_: RedisReply[_]).asList[A].flatten
  }

  case class EvalSHA[A](script: String, keys: List[Any], args: List[Any])(implicit format: Format, parse: Parse[A]) extends EvalCommand {
    type Ret = Option[A]
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args) map format.apply)
    val ret  = (_: RedisReply[_]).asBulk[A]
  }

  case class ScriptLoad(script: String) extends EvalCommand {
    type Ret = Option[String]
    def line = multiBulk("SCRIPT" +: Seq("LOAD", script))
    val ret  = (_: RedisReply[_]).asBulk[String]
  }
      
  import com.redis.serialization.Parse.Implicits._
  case class ScriptExists(shaHash: String) extends EvalCommand {
    type Ret = List[Int]
    def line = multiBulk("SCRIPT" +: Seq("EXISTS", shaHash))
    val ret  = (_: RedisReply[_]).asList[Int].flatten
  }

  case object ScriptFlush extends EvalCommand {
    type Ret = Boolean
    def line = multiBulk("SCRIPT" +: Seq("FLUSH"))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  private def argsForEval(luaCode: String, keys: List[Any], args: List[Any]): List[Any] =
    luaCode :: keys.length :: keys ::: args
}
