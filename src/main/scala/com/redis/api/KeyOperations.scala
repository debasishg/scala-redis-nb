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
  def keys[A](pattern: Any = "*")(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(Keys(pattern)).mapTo[Keys[A]#Ret]

  // RANDOMKEY
  // return a randomly selected key from the currently selected DB.
  def randomkey[A](implicit timeout: Timeout, parse: Parse[A]) =
    clientRef.ask(RandomKey[A]).mapTo[RandomKey[A]#Ret]

  // RENAME (oldkey, newkey)
  // atomically renames the key oldkey to newkey.
  def rename(oldkey: Any, newkey: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Rename(oldkey, newkey)).mapTo[Rename#Ret]

  // RENAMENX (oldkey, newkey)
  // rename oldkey into newkey but fails if the destination key newkey already exists.
  def renamenx(oldkey: Any, newkey: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Rename(oldkey, newkey, nx = true)).mapTo[Rename#Ret]

  // DBSIZE
  // return the size of the db.
  def dbsize()(implicit timeout: Timeout) =
    clientRef.ask(DBSize).mapTo[DBSize.Ret]

  // EXISTS (key)
  // test if the specified key exists.
  def exists(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Exists(key)).mapTo[Exists#Ret]

  // DELETE (key1 key2 ..)
  // deletes the specified keys.
  def del(key: Any, keys: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Del(key, keys:_*)).mapTo[Del#Ret]

  // TYPE (key)
  // return the type of the value stored at key in form of a string.
  def getType(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(GetType(key)).mapTo[GetType#Ret]

  // EXPIRE (key, expiry)
  // sets the expire time (in sec.) for the specified key.
  def expire(key: Any, ttl: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Expire(key, ttl)).mapTo[Expire#Ret]

  // PEXPIRE (key, expiry)
  // sets the expire time (in milli sec.) for the specified key.
  def pexpire(key: Any, ttlInMillis: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Expire(key, ttlInMillis, millis = true)).mapTo[Expire#Ret]

  // EXPIREAT (key, unix timestamp)
  // sets the expire time for the specified key.
  def expireat(key: Any, timestamp: Long)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ExpireAt(key, timestamp)).mapTo[ExpireAt#Ret]

  // PEXPIREAT (key, unix timestamp)
  // sets the expire timestamp in millis for the specified key.
  def pexpireat(key: Any, timestampInMillis: Long)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ExpireAt(key, timestampInMillis, millis = true)).mapTo[ExpireAt#Ret]

  // TTL (key)
  // returns the remaining time to live of a key that has a timeout
  def ttl(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(TTL(key)).mapTo[TTL#Ret]

  // PTTL (key)
  // returns the remaining time to live of a key that has a timeout in millis
  def pttl(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(TTL(key, millis = true)).mapTo[TTL#Ret]

  // FLUSHDB the DB
  // removes all the DB data.
  def flushdb()(implicit timeout: Timeout) =
    clientRef.ask(FlushDB(all = false)).mapTo[FlushDB#Ret]

  // FLUSHALL the DB's
  // removes data from all the DB's.
  def flushall()(implicit timeout: Timeout) =
    clientRef.ask(FlushDB(all = true)).mapTo[FlushDB#Ret]

  // MOVE
  // Move the specified key from the currently selected DB to the specified destination DB.
  def move(key: Any, db: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Move(key, db)).mapTo[Move#Ret]

  // QUIT
  // exits the server.
  def quit()(implicit timeout: Timeout): Future[Boolean] = {
    clientRef ! Quit
    gracefulStop(clientRef, 5 seconds)
  }

  // AUTH
  // auths with the server.
  def auth(secret: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Auth(secret)).mapTo[Auth#Ret]

  // PERSIST (key)
  // Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
  // to persistent (a key that will never expire as no timeout is associated).
  def persist(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Persist(key)).mapTo[Persist#Ret]

  // SORT
  // sort keys in a set, and optionally pull values for them
  def sort[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(Sort(key, limit, desc, alpha, by, get)).mapTo[Sort[A]#Ret]

  // SORT with STORE
  // sort keys in a set, and store result in the supplied key
  def sortNStore[A](key: String, 
    limit: Option[Pair[Int, Int]] = None, 
    desc: Boolean = false, 
    alpha: Boolean = false, 
    by: Option[String] = None, 
    get: List[String] = Nil,
    storeAt: String)(implicit timeout: Timeout, format:Format, parse:Parse[A]) =
    clientRef.ask(SortNStore(key, limit, desc, alpha, by, get, storeAt)).mapTo[SortNStore[A]#Ret]

  // SELECT (index)
  // selects the DB to connect, defaults to 0 (zero).
  def select(index: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Select(index)).mapTo[Select#Ret]
}
