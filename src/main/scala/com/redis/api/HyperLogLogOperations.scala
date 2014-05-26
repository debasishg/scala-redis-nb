package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.HyperLogLogCommands

trait HyperLogLogOperations { this: RedisOps =>
  import HyperLogLogCommands._

  def pfadd(key: String, elements: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(PFAdd(key, elements)).mapTo[PFAdd#Ret]

  def pfadd(key: String, element: String, elements: String*)(implicit timeout: Timeout) =
    clientRef.ask(PFAdd(key, element, elements: _*)).mapTo[PFAdd#Ret]

  def pfcount(keys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(PFCount(keys)).mapTo[PFCount#Ret]

  def pfcount(key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(PFCount(key, keys: _*)).mapTo[PFCount#Ret]

  def pfmerge(destKey: String, sourceKeys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(PFMerge(destKey, sourceKeys)).mapTo[PFMerge#Ret]

  def pfmerge(destKey: String, sourceKey: String, sourceKeys: String*)(implicit timeout: Timeout) =
    clientRef.ask(PFMerge(destKey, sourceKey, sourceKeys: _*)).mapTo[PFMerge#Ret]
}
