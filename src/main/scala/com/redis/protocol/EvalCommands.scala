package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object EvalCommands {

  case class EvalMultiBulk[A](script: String, keys: Seq[String], args: Seq[Stringified])
                             (implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args))
  }

  case class EvalBulk[A](script: String, keys: Seq[String], args: Seq[Stringified])
                        (implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args))
  }

  case class EvalMultiSHA[A](script: String, keys: Seq[String], args: Seq[Stringified])
                            (implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args))
  }

  case class EvalSHA[A](script: String, keys: Seq[String], args: Seq[Stringified])
                       (implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args))
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

  private def argsForEval[A](luaCode: String, keys: Seq[String], args: Seq[Stringified]): Seq[String] =
    luaCode +: keys.length.toString +: (keys ++ args.map(_.toString))
}
