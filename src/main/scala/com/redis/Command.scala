package com.redis

import scala.concurrent.Promise
import scala.util.Try
import ProtocolUtils._

sealed trait RedisCommand {
  // command returns Option[Ret]
  type Ret

  // command input : the request protocol of redis (upstream)
  val line: Array[Byte]

  // the promise which will be set by the command
  lazy val promise = Promise[Ret]

  // mapping of redis reply to the final return type
  val ret: Array[Byte] => Ret

  // processing pipeline (downstream)
  final def execute(s: Array[Byte]): Promise[Ret] = promise complete Try(ret(s))
}

trait StringCommand       extends RedisCommand
trait ListCommand         extends RedisCommand
trait KeyCommand          extends RedisCommand
trait SetCommand          extends RedisCommand
trait SortedSetCommand    extends RedisCommand
trait HashCommand         extends RedisCommand

object RedisCommand {
  trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder

  trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList
  
  def multiBulk(args: Seq[Array[Byte]]): Array[Byte] = {
    val b = new scala.collection.mutable.ArrayBuilder.ofByte
    b ++= "*%d".format(args.size).getBytes
    b ++= LS
    args foreach { arg =>
      b ++= "$%d".format(arg.size).getBytes
      b ++= LS
      b ++= arg
      b ++= LS
    }
    b.result
  }
}
