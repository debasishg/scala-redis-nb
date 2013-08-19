package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object EvalCommands {

  // @todo: Find a better way to unmarshal various types of arguments

  case class EvalMultiBulk[A, B](script: String, keys: Seq[String], args: Seq[A])
                                (implicit write: Write[A], read: Read[B]) extends RedisCommand[List[B]] {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args))
  }

  case class EvalBulk[A, B](script: String, keys: Seq[String], args: Seq[A])
                           (implicit write: Write[A], read: Read[B]) extends RedisCommand[Option[B]] {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args))
  }

  case class EvalMultiSHA[A, B](script: String, keys: Seq[String], args: Seq[A])
                               (implicit write: Write[A], read: Read[B]) extends RedisCommand[List[B]] {
    def line = multiBulk("EVALSHA" +: argsForEval(script, keys, args))
  }

  case class EvalSHA[A, B](script: String, keys: Seq[String], args: Seq[A])
                          (implicit write: Write[A], read: Read[B]) extends RedisCommand[Option[B]] {
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

  private def argsForEval[A](luaCode: String, keys: Seq[String], args: Seq[A])(implicit write: Write[A]): Seq[String] =
    luaCode +: keys.length.toString +: (keys ++ args.map(write))
}
