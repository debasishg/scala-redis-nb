package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object EvalCommands {

  case class Eval[A](script: String, keys: Seq[String], args: Seq[Stringified])
                        (implicit reader: Read[A]) extends RedisCommand[List[A]]()(PartialDeserializer.ensureListPD) {
    def line = multiBulk("EVAL" +: argsForEval(script, keys, args))
  }

  case class EvalSHA[A](shaHash: String, keys: Seq[String], args: Seq[Stringified])
                       (implicit reader: Read[A]) extends RedisCommand[List[A]]()(PartialDeserializer.ensureListPD) {
    def line = multiBulk("EVALSHA" +: argsForEval(shaHash, keys, args))
  }

  object Script {

    case class Load(script: String) extends RedisCommand[Option[String]] {
      def line = multiBulk("SCRIPT" +: Seq("LOAD", script))
    }

    case class Exists(shaHash: String) extends RedisCommand[List[Int]] {
      def line = multiBulk("SCRIPT" +: Seq("EXISTS", shaHash))
    }

    case object Flush extends RedisCommand[Boolean] {
      def line = multiBulk("SCRIPT" +: Seq("FLUSH"))
    }

  }

  private def argsForEval[A](luaCode: String, keys: Seq[String], args: Seq[Stringified]): Seq[String] =
    luaCode +: keys.length.toString +: (keys ++ args.map(_.toString))
}
