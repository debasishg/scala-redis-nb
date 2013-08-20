package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.EvalCommands

trait EvalOperations { this: RedisOps =>
  import EvalCommands._

  def evalMultiBulk[A](script: String, keys: Seq[String], args: Seq[Stringified])(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(EvalMultiBulk[A](script, keys, args)).mapTo[EvalMultiBulk[A]#Ret]

  def evalBulk[A](script: String, keys: Seq[String], args: Seq[Stringified])(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(EvalBulk[A](script, keys, args)).mapTo[EvalBulk[A]#Ret]

  def evalMultiSHA[A](script: String, keys: Seq[String], args: Seq[Stringified])(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(EvalMultiSHA[A](script, keys, args)).mapTo[EvalMultiSHA[A]#Ret]

  def evalSHA[A](script: String, keys: Seq[String], args: Seq[Stringified])(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(EvalSHA[A](script, keys, args)).mapTo[EvalSHA[A]#Ret]

  def scriptLoad(script: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptLoad(script)).mapTo[ScriptLoad#Ret]

  def scriptExists(shaHash: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptExists(shaHash)).mapTo[ScriptExists#Ret]

  def scriptFlush(implicit timeout: Timeout) =
    clientRef.ask(ScriptFlush).mapTo[ScriptFlush.Ret]
}
