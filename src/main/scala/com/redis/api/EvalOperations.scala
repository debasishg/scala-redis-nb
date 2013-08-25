package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.EvalCommands

trait EvalOperations { this: RedisOps =>
  import EvalCommands._

  def eval[A](script: String, keys: Seq[String] = Nil, args: Seq[Stringified] = Nil)
             (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(Eval[A](script, keys, args)).mapTo[Eval[A]#Ret]

  def evalsha[A](shaHash: String, keys: Seq[String] = Nil, args: Seq[Stringified] = Nil)
                (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(EvalSHA[A](shaHash, keys, args)).mapTo[EvalSHA[A]#Ret]

  // Sub-commands of SCRIPT
  object script {
    import Script._

    def load(script: String)(implicit timeout: Timeout) =
    clientRef.ask(Load(script)).mapTo[Load#Ret]

    def exists(shaHash: String)(implicit timeout: Timeout) =
      clientRef.ask(Exists(shaHash)).mapTo[Exists#Ret]

    def flush(implicit timeout: Timeout) =
      clientRef.ask(Flush).mapTo[Flush.Ret]
  }
}
