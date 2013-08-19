package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.EvalCommands

trait EvalOperations { this: RedisOps =>
  import EvalCommands._

  def evalMultiBulk[A, B](script: String, keys: Seq[String], args: Seq[A])(implicit timeout: Timeout, write: Write[A], parse: Read[B]) =
    clientRef.ask(EvalMultiBulk[A, B](script, keys, args)).mapTo[EvalMultiBulk[A, B]#Ret]

  def evalBulk[A, B](script: String, keys: Seq[String], args: Seq[A])(implicit timeout: Timeout, write: Write[A], parse: Read[B]) =
    clientRef.ask(EvalBulk[A, B](script, keys, args)).mapTo[EvalBulk[A, B]#Ret]

  def evalMultiSHA[A, B](script: String, keys: Seq[String], args: Seq[A])(implicit timeout: Timeout, write: Write[A], parse: Read[B]) =
    clientRef.ask(EvalMultiSHA[A, B](script, keys, args)).mapTo[EvalMultiSHA[A, B]#Ret]

  def evalSHA[A, B](script: String, keys: Seq[String], args: Seq[A])(implicit timeout: Timeout, write: Write[A], parse: Read[B]) =
    clientRef.ask(EvalSHA[A, B](script, keys, args)).mapTo[EvalSHA[A, B]#Ret]

  def scriptLoad(script: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptLoad(script)).mapTo[ScriptLoad#Ret]

  def scriptExists(shaHash: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptExists(shaHash)).mapTo[ScriptExists#Ret]

  def scriptFlush(implicit timeout: Timeout) =
    clientRef.ask(ScriptFlush).mapTo[ScriptFlush.Ret]
}
