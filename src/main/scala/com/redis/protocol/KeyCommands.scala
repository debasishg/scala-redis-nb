package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object KeyCommands {
  case class Keys(pattern: String = "*") extends RedisCommand[List[String]] {
    def line = multiBulk("KEYS" +: List(pattern))
  }

  case object RandomKey extends RedisCommand[Option[String]] {
    def line = multiBulk(List("RANDOMKEY"))
  }

  case class Rename(oldKey: String, newKey: String, nx: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk((if (nx) "RENAMENX" else "RENAME") +: List(oldKey, newKey))
  }

  case object DBSize extends RedisCommand[Long] {
    def line = multiBulk(List("DBSIZE"))
  }

  case class Exists(key: String) extends RedisCommand[Boolean] {
    def line = multiBulk("EXISTS" :: List(key))
  }

  case class Del(key: String, keys: String*) extends RedisCommand[Long] {
    def line = multiBulk("DEL" :: key :: keys.toList)
  }

  case class GetType(key: String) extends RedisCommand[String] {
    def line = multiBulk("TYPE" :: List(key))
  }

  case class Expire(key: String, ttl: Int, millis: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk((if (millis) "PEXPIRE" else "EXPIRE") :: List(key, ttl.toString))
  }

  case class ExpireAt(key: String, timestamp: Long, millis: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk((if (millis) "PEXPIREAT" else "EXPIREAT") :: List(key, timestamp.toString))
  }

  case class TTL(key: String, millis: Boolean = false) extends RedisCommand[Long] {
    def line = multiBulk((if (millis) "PTTL" else "TTL") :: List(key))
  }

  case class FlushDB(all: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk(List(if (all) "FLUSHALL" else "FLUSHDB"))
  }

  case class Move(key: String, db: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("MOVE" :: List(key, db.toString))
  }

  case object Quit extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("QUIT"))
  }

  case class Auth(secret: String) extends RedisCommand[Boolean] {
    def line = multiBulk("AUTH" :: List(secret))
  }

  case class Persist(key: String) extends RedisCommand[Boolean] {
    def line = multiBulk("PERSIST" :: List(key))
  }

  case class Sort[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil)(implicit reader: Read[A]) extends RedisCommand[List[A]] {

    def line = multiBulk("SORT" :: makeSortArgs(key, limit, desc, alpha, by, get))
  }

  case class SortNStore(key: String,
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil,
    storeAt: String) extends RedisCommand[Long] {

    def line = multiBulk("SORT" :: makeSortArgs(key, limit, desc, alpha, by, get) ::: List("STORE", storeAt))
  }

  private def makeSortArgs(key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil): List[String] = {

    List(List(key), limit.map(l => List("LIMIT", l._1.toString, l._2.toString)).getOrElse(Nil)
      , (if (desc) List("DESC") else Nil)
      , (if (alpha) List("ALPHA") else Nil)
      , by.map(b => List("BY", b)).getOrElse(Nil)
      , get.map(g => List("GET", g)).flatten
    ).flatten
  }

  case class Select(index: Int) extends RedisCommand[Boolean] {
    val line = multiBulk("SELECT" :: List(index.toString))
  }
}
