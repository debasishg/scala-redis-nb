package com.redis

import serialization._
import Parse.{Implicits => Parsers}


/**
 * Redis will reply to commands with different kinds of replies. It is always possible to detect the kind of reply
 * from the first byte sent by the server:
 * <li> In a Status Reply the first byte of the reply is "+"</li>
 * <li> In an Error Reply the first byte of the reply is "-"</li>
 * <li> In an Integer Reply the first byte of the reply is ":"</li>
 * <li> In a Bulk Reply the first byte of the reply is "$"</li>
 * <li> In a Multi Bulk Reply the first byte of the reply s "*"</li>
 */

class RedisReply(value: Any) {

  def asString: String = value.asInstanceOf[String]

  def asBulk[T: Parse]: Option[T] = value.asInstanceOf[Option[String]] map implicitly[Parse[T]]

  def asLong: Long =  value.asInstanceOf[Long]

  def asBoolean: Boolean = value match {
    case n: Long => n > 0
    case s: String => s match {
      case "OK" => true
      case "QUEUED" => true
      case _ => false
    }
    case x => false
  }

  def asList[T: Parse]: List[Option[T]] = value.asInstanceOf[List[Option[String]]].map(_.map(implicitly[Parse[T]]))

  def asListPairs[A: Parse, B: Parse]: List[Option[(A,B)]] = {
    val parseA = implicitly[Parse[A]]
    val parseB = implicitly[Parse[B]]

    value.asInstanceOf[List[Option[String]]].grouped(2).flatMap{
      case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
      case _ => Iterator.single(None)
    }.toList
  }

  def asQueuedList: List[Option[String]] = asList[String]

  // def asExec(handlers: Seq[() => Any]): Option[List[Any]] = receive(execReply(handlers))

  def asSet[T: Parse]: collection.immutable.Set[T] = asList.flatten.toSet

  def asAny = value
}

object RedisReply {
  def apply(value: Any) = new RedisReply(value)
}
