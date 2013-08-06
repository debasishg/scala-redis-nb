package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.EvalCommands

trait EvalOperations { this: RedisOps =>
  import EvalCommands._

  def evalMultiBulk[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalMultiBulk(script, keys, args)).mapTo[EvalMultiBulk[A]#Ret]

  def evalBulk[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalBulk(script, keys, args)).mapTo[EvalBulk[A]#Ret]

  def evalMultiSHA[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalMultiSHA(script, keys, args)).mapTo[EvalMultiSHA[A]#Ret]

  def evalSHA[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalSHA(script, keys, args)).mapTo[EvalSHA[A]#Ret]

  def scriptLoad(script: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptLoad(script)).mapTo[ScriptLoad#Ret]

  def scriptExists(shaHash: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptExists(shaHash)).mapTo[ScriptExists#Ret]

  def scriptFlush(implicit timeout: Timeout) =
    clientRef.ask(ScriptFlush).mapTo[ScriptFlush.Ret]
}
