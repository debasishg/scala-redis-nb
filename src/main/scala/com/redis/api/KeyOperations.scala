package com.redis
package api

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import akka.pattern.gracefulStop
import com.redis.protocol._


trait KeyOperations { this: RedisOps =>
  import KeyCommands._

  // KEYS
  // returns all the keys matching the glob-style pattern.
  def keys(pattern: String = "*")(implicit timeout: Timeout) =
    clientRef.ask(Keys(pattern)).mapTo[Keys#Ret]

  // RANDOMKEY
  // return a randomly selected key from the currently selected DB.
  def randomkey(implicit timeout: Timeout) =
    clientRef.ask(RandomKey).mapTo[RandomKey.Ret]

  // RENAME (oldkey, newkey)
  // atomically renames the key oldkey to newkey.
  def rename(oldkey: String, newkey: String)(implicit timeout: Timeout) =
    clientRef.ask(Rename(oldkey, newkey)).mapTo[Rename#Ret]

  // RENAMENX (oldkey, newkey)
  // rename oldkey into newkey but fails if the destination key newkey already exists.
  def renamenx(oldkey: String, newkey: String)(implicit timeout: Timeout) =
    clientRef.ask(RenameNx(oldkey, newkey)).mapTo[RenameNx#Ret]

  // DBSIZE
  // return the size of the db.
  def dbsize()(implicit timeout: Timeout) =
    clientRef.ask(DBSize).mapTo[DBSize.Ret]

  // EXISTS (key)
  // test if the specified key exists.
  def exists(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Exists(key)).mapTo[Exists#Ret]


  // DELETE (key1 key2 ..)
  // deletes the specified keys.
  def del(keys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(Del(keys)).mapTo[Del#Ret]

  def del(key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(Del(key, keys:_*)).mapTo[Del#Ret]


  // TYPE (key)
  // return the type of the value stored at key in form of a string.
  def `type`(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Type(key)).mapTo[Type#Ret]

  def tpe(key: String)(implicit timeout: Timeout) = `type`(key)


  // EXPIRE (key, expiry)
  // sets the expire time (in sec.) for the specified key.
  def expire(key: String, ttl: Int)(implicit timeout: Timeout) =
    clientRef.ask(Expire(key, ttl)).mapTo[Expire#Ret]

  // PEXPIRE (key, expiry)
  // sets the expire time (in milli sec.) for the specified key.
  def pexpire(key: String, ttlInMillis: Int)(implicit timeout: Timeout) =
    clientRef.ask(PExpire(key, ttlInMillis)).mapTo[PExpire#Ret]

  // EXPIREAT (key, unix timestamp)
  // sets the expire time for the specified key.
  def expireat(key: String, timestamp: Long)(implicit timeout: Timeout) =
    clientRef.ask(ExpireAt(key, timestamp)).mapTo[ExpireAt#Ret]

  // PEXPIREAT (key, unix timestamp)
  // sets the expire timestamp in millis for the specified key.
  def pexpireat(key: String, timestampInMillis: Long)(implicit timeout: Timeout) =
    clientRef.ask(PExpireAt(key, timestampInMillis)).mapTo[PExpireAt#Ret]

  // TTL (key)
  // returns the remaining time to live of a key that has a timeout
  def ttl(key: String)(implicit timeout: Timeout) =
    clientRef.ask(TTL(key)).mapTo[TTL#Ret]

  // PTTL (key)
  // returns the remaining time to live of a key that has a timeout in millis
  def pttl(key: String)(implicit timeout: Timeout) =
    clientRef.ask(PTTL(key)).mapTo[PTTL#Ret]

  // FLUSHDB the DB
  // removes all the DB data.
  def flushdb()(implicit timeout: Timeout) =
    clientRef.ask(FlushDB).mapTo[FlushDB.Ret]

  // FLUSHALL the DB's
  // removes data from all the DB's.
  def flushall()(implicit timeout: Timeout) =
    clientRef.ask(FlushAll).mapTo[FlushAll.Ret]

  // MOVE
  // Move the specified key from the currently selected DB to the specified destination DB.
  def move(key: String, db: Int)(implicit timeout: Timeout) =
    clientRef.ask(Move(key, db)).mapTo[Move#Ret]

  // QUIT
  // exits the server.
  def quit()(implicit timeout: Timeout): Future[Boolean] = {
    clientRef ! Quit
    gracefulStop(clientRef, 5 seconds)
  }

  // AUTH
  // auths with the server.
  def auth(secret: String)(implicit timeout: Timeout) =
    clientRef.ask(Auth(secret)).mapTo[Auth#Ret]

  // PERSIST (key)
  // Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
  // to persistent (a key that will never expire as no timeout is associated).
  def persist(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Persist(key)).mapTo[Persist#Ret]

  // SORT
  // sort keys in a set, and optionally pull values for them
  def sort[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: Seq[String] = Nil)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(Sort(key, limit, desc, alpha, by, get)).mapTo[Sort[A]#Ret]

  // SORT with STORE
  // sort keys in a set, and store result in the supplied key
  def sortNStore(key: String,
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: Seq[String] = Nil,
    storeAt: String)(implicit timeout: Timeout) =
    clientRef.ask(SortNStore(key, limit, desc, alpha, by, get, storeAt)).mapTo[SortNStore#Ret]

  // SELECT (index)
  // selects the DB to connect, defaults to 0 (zero).
  def select(index: Int)(implicit timeout: Timeout) =
    clientRef.ask(Select(index)).mapTo[Select#Ret]
}
