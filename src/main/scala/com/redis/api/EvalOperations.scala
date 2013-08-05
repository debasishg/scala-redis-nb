package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.EvalCommands

trait EvalOperations { this: RedisOps =>
  import EvalCommands._

  def evalMultiBulk[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalMultiBulk(script, keys, args)).mapTo[List[A]]

  def evalBulk[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalBulk(script, keys, args)).mapTo[Option[A]]

  def evalMultiSHA[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalMultiSHA(script, keys, args)).mapTo[List[A]]

  def evalSHA[A](script: String, keys: List[Any], args: List[Any])(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(EvalSHA(script, keys, args)).mapTo[Option[A]]

  def scriptLoad(script: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptLoad(script)).mapTo[Option[String]]

  def scriptExists(shaHash: String)(implicit timeout: Timeout) =
    clientRef.ask(ScriptExists(shaHash)).mapTo[List[Int]]

  def scriptFlush(implicit timeout: Timeout) =
    clientRef.ask(ScriptFlush).mapTo[Boolean]
}
