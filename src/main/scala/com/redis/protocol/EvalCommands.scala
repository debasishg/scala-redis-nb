package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object EvalCommands {

  case class EvalMultiBulk[A](script: String, keys: List[Any], args: List[Any])
                             (implicit format: Format, parse: Parse[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args) map format.apply)
  }

  case class EvalBulk[A](script: String, keys: List[Any], args: List[Any])
                        (implicit format: Format, parse: Parse[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args) map format.apply)
  }

  case class EvalMultiSHA[A](script: String, keys: List[Any], args: List[Any])
                            (implicit format: Format, parse: Parse[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args) map format.apply)
  }

  case class EvalSHA[A](script: String, keys: List[Any], args: List[Any])
                       (implicit format: Format, parse: Parse[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args) map format.apply)
  }

  case class ScriptLoad(script: String) extends RedisCommand[Option[String]] {
    def line = multiBulk("SCRIPT" +: Seq("LOAD", script))
  }

  case class ScriptExists(shaHash: String) extends RedisCommand[List[Int]] {
    def line = multiBulk("SCRIPT" +: Seq("EXISTS", shaHash))
  }

  case object ScriptFlush extends RedisCommand[Boolean] {
    def line = multiBulk("SCRIPT" +: Seq("FLUSH"))
  }

  private def argsForEval(luaCode: String, keys: List[Any], args: List[Any]): List[Any] =
    luaCode :: keys.length :: keys ::: args
}
