package com.redis.protocol

import com.redis.serialization._


object KeyCommands {
  import DefaultWriters._

  case class Keys(pattern: String = "*") extends RedisCommand[List[String]]("KEYS") {
    def params = pattern +: ANil
  }

  case class Scan(cursor:Long = 0, pattern:String = "", count:Long = 0) extends RedisCommand[(Long, List[String])]("SCAN") {
    require(cursor >= 0)
    def params = Seq( Seq(cursor.toString),
      if(pattern != "") Seq("MATCH", pattern) else Nil,
      if(count > 0) Seq("COUNT", count.toString) else Nil
    ).flatten.toArgs
  }

  case object RandomKey extends RedisCommand[Option[String]]("RANDOMKEY") {
    def params = ANil
  }

  case class Rename(oldKey: String, newKey: String) extends RedisCommand[Boolean]("RENAME") {
    def params = oldKey +: newKey +: ANil
  }

  case class RenameNx(oldKey: String, newKey: String) extends RedisCommand[Boolean]("RENAMENX") {
    def params = oldKey +: newKey +: ANil
  }

  case object DBSize extends RedisCommand[Long]("DBSIZE") {
    def params = ANil
  }

  case class Exists(key: String) extends RedisCommand[Boolean]("EXISTS") {
    def params = key +: ANil
  }


  case class Del(keys: Seq[String]) extends RedisCommand[Long]("DEL") {
    require(keys.nonEmpty, "Keys should not be empty")
    def params = keys.toArgs
  }

  object Del {
    def apply(key: String, keys: String*): Del = Del(key +: keys)
  }


  case class Type(key: String) extends RedisCommand[String]("TYPE") {
    def params = key +: ANil
  }

  case class Expire(key: String, ttl: Int) extends RedisCommand[Boolean]("EXPIRE") {
    def params = key +: ttl +: ANil
  }

  case class PExpire(key: String, ttl: Int) extends RedisCommand[Boolean]("PEXPIRE") {
    def params = key +: ttl +: ANil
  }

  case class ExpireAt(key: String, timestamp: Long) extends RedisCommand[Boolean]("EXPIREAT") {
    def params = key +: timestamp +: ANil
  }

  case class PExpireAt(key: String, timestamp: Long) extends RedisCommand[Boolean]("PEXPIREAT") {
    def params = key +: timestamp +: ANil
  }

  case class TTL(key: String) extends RedisCommand[Long]("TTL") {
    def params = key +: ANil
  }

  case class PTTL(key: String) extends RedisCommand[Long]("PTTL") {
    def params = key +: ANil
  }

  case object FlushDB extends RedisCommand[Boolean]("FLUSHDB") {
    def params = ANil
  }

  case object FlushAll extends RedisCommand[Boolean]("FLUSHALL") {
    def params = ANil
  }

  case class Move(key: String, db: Int) extends RedisCommand[Boolean]("MOVE") {
    def params = key +: db +: ANil
  }

  case class Persist(key: String) extends RedisCommand[Boolean]("PERSIST") {
    def params = key +: ANil
  }

  case class Sort[A: Reader](key: String,
    limit: Option[Tuple2[Int, Int]] = None,
    desc: Boolean = false,
    alpha: Boolean = false,
    by: Option[String] = None,
    get: Seq[String] = Nil) extends RedisCommand[List[A]]("SORT") {

    def params = makeSortArgs(key, limit, desc, alpha, by, get)
  }

  case class SortNStore(key: String,
    limit: Option[Tuple2[Int, Int]] = None,
    desc: Boolean = false,
    alpha: Boolean = false,
    by: Option[String] = None,
    get: Seq[String] = Nil,
    storeAt: String) extends RedisCommand[Long]("SORT") {

    def params = makeSortArgs(key, limit, desc, alpha, by, get) ++ Seq("STORE", storeAt)
  }

  private def makeSortArgs(key: String,
    limit: Option[Tuple2[Int, Int]] = None,
    desc: Boolean = false,
    alpha: Boolean = false,
    by: Option[String] = None,
    get: Seq[String] = Nil): Args = {

    Seq(Seq(key)
      , limit.fold(Seq.empty[String]) { case (from, to) => "LIMIT" +: Seq(from, to).map(_.toString) }
      , if (desc) Seq("DESC") else Nil
      , if (alpha) Seq("ALPHA") else Nil
      , by.fold(Seq.empty[String])("BY" +: _ +: Nil)
      , get.map("GET" +: _ +: Nil).flatten
    ).flatten.toArgs
  }

  case class Select(index: Int) extends RedisCommand[Boolean]("SELECT") {
    def params = index +: ANil
  }

}
