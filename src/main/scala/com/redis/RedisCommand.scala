package com.redis

import scala.concurrent.Promise
import scala.util.Try
import ProtocolUtils._
import akka.util.{ByteString, ByteStringBuilder}


sealed trait RedisCommand {
  // command returns Option[Ret]
  type Ret

  // command input : the request protocol of redis (upstream)
  val line: ByteString

  // the promise which will be set by the command
  lazy val promise = Promise[Ret]

  // mapping of redis reply to the final return type
  val ret: ByteString => Ret

  // processing pipeline (downstream)
  final def execute(s: ByteString): Promise[Ret] = promise complete Try(ret(s))
}

trait StringCommand       extends RedisCommand
trait ListCommand         extends RedisCommand
trait KeyCommand          extends RedisCommand
trait SetCommand          extends RedisCommand
trait SortedSetCommand    extends RedisCommand
trait HashCommand         extends RedisCommand
trait NodeCommand         extends RedisCommand

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
  
  def multiBulk(args: Seq[String]): ByteString = {
    val b = new ByteStringBuilder
    b += Multi
    b ++= ByteString(args.size.toString)
    b ++= LS
    args foreach { arg =>
      b += Bulk
      b ++= ByteString(arg.size.toString)
      b ++= LS
      b ++= ByteString(arg)
      b ++= LS
    }
    b.result
  }
}
