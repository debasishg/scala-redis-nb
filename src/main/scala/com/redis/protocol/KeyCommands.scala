package com.redis.protocol

import com.redis.serialization.{PartialDeserializer, Read}
import RedisCommand._


object KeyCommands {
  case class Keys(pattern: String = "*") extends RedisCommand[List[String]] {
    def line = multiBulk("KEYS" +: Seq(pattern))
  }

  case object RandomKey extends RedisCommand[Option[String]] {
    def line = multiBulk(Seq("RANDOMKEY"))
  }

  case class Rename(oldKey: String, newKey: String, nx: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk((if (nx) "RENAMENX" else "RENAME") +: Seq(oldKey, newKey))
  }

  case object DBSize extends RedisCommand[Long] {
    def line = multiBulk(Seq("DBSIZE"))
  }

  case class Exists(key: String) extends RedisCommand[Boolean] {
    def line = multiBulk("EXISTS" +: Seq(key))
  }


  case class Del(keys: Seq[String]) extends RedisCommand[Long] {
    require(keys.nonEmpty)
    def line = multiBulk("DEL" +: keys)
  }

  object Del {
    def apply(key: String, keys: String*): Del = Del(key +: keys)
  }


  case class Type(key: String) extends RedisCommand[String] {
    def line = multiBulk("TYPE" +: Seq(key))
  }

  case class Expire(key: String, ttl: Int, millis: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk((if (millis) "PEXPIRE" else "EXPIRE") +: Seq(key, ttl.toString))
  }

  case class ExpireAt(key: String, timestamp: Long, millis: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk((if (millis) "PEXPIREAT" else "EXPIREAT") +: Seq(key, timestamp.toString))
  }

  case class TTL(key: String, millis: Boolean = false) extends RedisCommand[Long] {
    def line = multiBulk((if (millis) "PTTL" else "TTL") +: Seq(key))
  }

  case class FlushDB(all: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk(Seq(if (all) "FLUSHALL" else "FLUSHDB"))
  }

  case class Move(key: String, db: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("MOVE" +: Seq(key, db.toString))
  }

  case object Quit extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("QUIT"))
  }

  case class Auth(secret: String) extends RedisCommand[Boolean] {
    def line = multiBulk("AUTH" +: Seq(secret))
  }

  case class Persist(key: String) extends RedisCommand[Boolean] {
    def line = multiBulk("PERSIST" +: Seq(key))
  }

  case class Sort[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: Seq[String] = Nil)(implicit reader: Read[A]) extends RedisCommand[List[A]] {

    def line = multiBulk("SORT" +: makeSortArgs(key, limit, desc, alpha, by, get))
  }

  case class SortNStore(key: String,
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: Seq[String] = Nil,
    storeAt: String) extends RedisCommand[Long] {

    def line = multiBulk("SORT" +: (makeSortArgs(key, limit, desc, alpha, by, get) ++ Seq("STORE", storeAt)))
  }

  private def makeSortArgs(key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: Seq[String] = Nil): Seq[String] = {

    Seq(Seq(key)
      , limit.fold(Seq.empty[String]) { case (from, to) => "LIMIT" +: Seq(from, to).map(_.toString) }
      , if (desc) Seq("DESC") else Nil
      , if (alpha) Seq("ALPHA") else Nil
      , by.fold(Seq.empty[String])("BY" +: _ +: Nil)
      , get.map("GET" +: _ +: Nil).flatten
    ).flatten
  }

  case class Select(index: Int) extends RedisCommand[Boolean] {
    val line = multiBulk("SELECT" +: Seq(index.toString))
  }
}
