package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object KeyCommands {
  case class Keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]) extends KeyCommand {
    type Ret = List[A]
    def line = multiBulk("KEYS" +: Seq(format.apply(pattern)))
    val ret  = (_: RedisReply[_]).asList.flatten // TODO remove intermediate Option
  }

  case class RandomKey[A](implicit parse: Parse[A]) extends KeyCommand {
    type Ret = Option[A]
    def line = multiBulk(Seq("RANDOMKEY"))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }

  case class Rename(oldKey: Any, newKey: Any, nx: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk((if (nx) "RENAMENX" else "RENAME") +: (Seq(oldKey, newKey) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case object DBSize extends KeyCommand {
    type Ret = Long
    def line = multiBulk(Seq("DBSIZE"))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class Exists(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk("EXISTS" +: Seq(format.apply(key)))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class Del(key: Any, keys: Any*)(implicit format: Format) extends KeyCommand {
    type Ret = Long
    def line = multiBulk("DEL" +: ((key :: keys.toList) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class GetType(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = String
    def line = multiBulk("TYPE" +: Seq(format.apply(key)))
    val ret  = (_: RedisReply[_]).asString
  }

  case class Expire(key: Any, ttl: Int, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk((if (millis) "PEXPIRE" else "EXPIRE") +: (Seq(key, ttl) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class ExpireAt(key: Any, timestamp: Long, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk((if (millis) "PEXPIREAT" else "EXPIREAT") +: (Seq(key, timestamp) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class TTL(key: Any, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Long
    def line = multiBulk((if (millis) "PTTL" else "TTL") +: Seq(format.apply(key)))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class FlushDB(all: Boolean = false) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk(Seq(if (all) "FLUSHALL" else "FLUSHDB"))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class Move(key: Any, db: Int)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk("MOVE" +: (Seq(key, db) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case object Quit extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk(Seq("QUIT"))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class Auth(secret: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk("AUTH" +: (Seq(secret) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class Persist(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    def line = multiBulk("PERSIST" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class Sort[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil)(implicit format: Format, parse: Parse[A]) extends KeyCommand {

    type Ret = List[A]
    val commands: Seq[Any] = makeSortArgs(key, limit, desc, alpha, by, get)
    def line = multiBulk("SORT" +: (commands map format.apply))
    val ret  = (_: RedisReply[_]).asList[A].flatten
  }

  case class SortNStore[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil,
    storeAt: String)(implicit format: Format, parse: Parse[A]) extends KeyCommand {

    type Ret = Long
    val commands: Seq[Any] = makeSortArgs(key, limit, desc, alpha, by, get) ::: List("STORE", storeAt)
    def line = multiBulk("SORT" +: (commands map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  private def makeSortArgs(key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil): List[Any] = {

    List(List(key), limit.map(l => List("LIMIT", l._1, l._2)).getOrElse(Nil)
      , (if (desc) List("DESC") else Nil)
      , (if (alpha) List("ALPHA") else Nil)
      , by.map(b => List("BY", b)).getOrElse(Nil)
      , get.map(g => List("GET", g)).flatMap(x=>x)
    ).flatMap(x=>x)
  }

  // case class Select ..
}
