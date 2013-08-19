package com.redis.protocol

import akka.util.{ByteString, ByteStringBuilder}
import com.redis.serialization.{Write, PartialDeserializer}


abstract class RedisCommand[A]()(implicit _des: PartialDeserializer[A]) {

  type Ret = A

  // command input : the request protocol of redis (upstream)
  def line: ByteString

  def des = _des
}

object RedisCommand {

  trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder

  trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  def flattenPairs[A](in: Iterable[Product2[String, A]])(implicit write: Write[A]): List[String] =
    in.iterator.flatMap(x => Iterator(x._1, write(x._2))).toList
  
  def multiBulk(args: Seq[String]): ByteString = {
    val b = new ByteStringBuilder
    b += Multi
    b ++= ByteString(args.size.toString)
    b ++= Newline
    args foreach { arg =>
      b += Bulk
      b ++= ByteString(arg.size.toString)
      b ++= Newline
      b ++= ByteString(arg)
      b ++= Newline
    }
    b.result
  }
}
