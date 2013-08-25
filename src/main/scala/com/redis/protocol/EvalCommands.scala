package com.redis.protocol

import com.redis.serialization._


object EvalCommands {
  import DefaultWriters._

  case class Eval[A: Reader](script: String, keys: Seq[String] = Nil, args: Seq[Stringified] = Nil)
      extends RedisCommand[List[A]]("EVAL")(PartialDeserializer.ensureListPD) {

    def params = argsForEval(script, keys, args)
  }

  case class EvalSHA[A: Reader](shaHash: String, keys: Seq[String] = Nil, args: Seq[Stringified] = Nil)
      extends RedisCommand[List[A]]("EVALSHA")(PartialDeserializer.ensureListPD) {

    def params = argsForEval(shaHash, keys, args)
  }

  object Script {

    case class Load(script: String) extends RedisCommand[Option[String]]("SCRIPT") {
      def params = "LOAD" +: script +: ANil
    }

    case class Exists(shaHash: String) extends RedisCommand[List[Int]]("SCRIPT") {
      def params = "EXISTS" +: shaHash +: ANil
    }

    case object Flush extends RedisCommand[Boolean]("SCRIPT") {
      def params = "Flush" +: ANil
    }

  }

  private def argsForEval[A](luaCode: String, keys: Seq[String], args: Seq[Stringified]): Args =
    luaCode +: keys.length +: keys ++: args.toArgs
}
