package com.redis
package api

import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure}
import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.{TransactionCommands, Discarded}
import ExecutionContext.Implicits.global

trait TransactionOperations { this: RedisOps =>
  import TransactionCommands._

  def multi()(implicit timeout: Timeout) =
    clientRef.ask(Multi).mapTo[Multi.type#Ret]

  def exec()(implicit timeout: Timeout) =
    clientRef.ask(Exec).mapTo[Exec.type#Ret]

  def discard()(implicit timeout: Timeout) =
    clientRef.ask(Discard).mapTo[Discard.type#Ret].map(_ => Discarded)

  def watch(keys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(Watch(keys)).mapTo[Watch#Ret]

  def watch(key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(Watch(key, keys:_*)).mapTo[Watch#Ret]

  def unwatch()(implicit timeout: Timeout) =
    clientRef.ask(Unwatch).mapTo[Unwatch.type#Ret]

  def withTransaction(txn: RedisOps => Unit)(implicit timeout: Timeout) = {
    multi()
    try {
      txn(this)
      exec()
    } catch {
      case th: Throwable => discard()
    }
  }
}
