package com.redis

import serialization._
import Parse.{Implicits => Parsers}
import ProtocolUtils._

object RedisReplies {

  /**
   * Redis will reply to commands with different kinds of replies. It is always possible to detect the kind of reply 
   * from the first byte sent by the server:
   * <li> In a Status Reply the first byte of the reply is "+"</li>
   * <li> In an Error Reply the first byte of the reply is "-"</li>
   * <li> In an Integer Reply the first byte of the reply is ":"</li>
   * <li> In a Bulk Reply the first byte of the reply is "$"</li>
   * <li> In a Multi Bulk Reply the first byte of the reply s "*"</li>
   */

  type Reply[T] = PartialFunction[(Char, Array[Byte], RedisReply), T]
  type SingleReply = Reply[Array[Byte]]
  type BulkReply = Reply[Option[Array[Byte]]]
  type MultiReply = Reply[List[Option[Array[Byte]]]]
  val crlf = List(13, 10)

  case class RedisReply(s: Array[Byte]) {
    val iter = split(s).iterator
    def get: Option[Array[Byte]] = {
      if (iter.hasNext) Some(iter.next)
      else None
    }

    def receive[T](pf: Reply[T]) = get match {
      case Some(line) =>
        (pf orElse errReply) apply ((line(0).toChar,line.slice(1,line.length), this))
      case None => sys.error("Error in receive")
    }

    def asString: String = Parsers.parseString(receive(singleLineReply)) 

    def asBulk[T: Parse]: Option[T] =  receive(bulkReply) map implicitly[Parse[T]]
  
    def asBulkWithTime[T](implicit parse: Parse[T]): Option[T] = receive(bulkReply orElse multiBulkReply) match {
      case x: Some[Array[Byte]] => x map parse
      case _ => None
    }

    def asLong: Long =  receive(longReply orElse queuedReplyLong)

    def asBoolean: Boolean = receive(longReply orElse singleLineReply) match {
      case n: Int => n > 0
      case s: Array[Byte] => Parsers.parseString(s) match {
        case "OK" => true
        case "QUEUED" => true
        case _ => false
      }
      case x => false
    }

    def asList[T: Parse]: List[Option[T]] = receive(multiBulkReply).map(_.map(implicitly[Parse[T]]))

    def asListPairs[A: Parse, B: Parse]: List[Option[(A,B)]] = {
      val parseA = implicitly[Parse[A]]
      val parseB = implicitly[Parse[B]]

      receive(multiBulkReply).grouped(2).flatMap{
        case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
        case _ => Iterator.single(None)
      }.toList
    }

    def asQueuedList: List[Option[String]] = receive(queuedReplyList).map(_.map(Parsers.parseString))

    // def asExec(handlers: Seq[() => Any]): Option[List[Any]] = receive(execReply(handlers))

    def asSet[T: Parse]: collection.immutable.Set[T] = asList.flatten.toSet

    def asAny = receive(longReply orElse singleLineReply orElse bulkReply orElse multiBulkReply)
  }

  val longReply: Reply[Long] = {
    case (INT, s, _) => Parsers.parseLong(s)
    case (BULK, s, _) if Parsers.parseInt(s) == -1 => -1L
  }

  val singleLineReply: SingleReply = {
    case (SINGLE, s, _) => s
    case (INT, s, _) => s
  }

  val bulkReply: BulkReply = {
    case (BULK, s, r) => 
      val next = r.get
      Parsers.parseInt(s) match {
        case -1 => None
        case x if x == next.get.size => next
        case _ => None
      }
  }

  val multiBulkReply: MultiReply = {
    case (MULTI, str, r) =>
      Parsers.parseInt(str) match {
        case -1 => List.empty
        case n => List.fill(n)(r.receive(bulkReply))
      }
  }

  val errReply: Reply[Nothing] = {
    case (ERR, s, _) => throw new Exception(Parsers.parseString(s))
    case x => throw new Exception("Protocol error: Got " + x + " as initial reply byte")
  }

  def execReply(handlers: Seq[() => Any]): PartialFunction[(Char, Array[Byte]), Option[List[Any]]] = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n if n == handlers.size => 
          Some(handlers.map(_.apply).toList)
        case n => throw new Exception("Protocol error: Expected "+handlers.size+" results, but got "+n)
      }
  }

  def queuedReplyInt: Reply[Int] = {
    case (SINGLE, QUEUED, _) => Int.MaxValue
  }
  
  def queuedReplyLong: Reply[Long] = {
    case (SINGLE, QUEUED, _) => Long.MaxValue
    }

  def queuedReplyList: MultiReply = {
    case (SINGLE, QUEUED, _) => List(Some(QUEUED))
  }
}
