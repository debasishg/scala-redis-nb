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
    clientRef.ask(Keys(pattern)).mapTo[List[A]]

  // RANDOMKEY
  // return a randomly selected key from the currently selected DB.
  def randomkey[A](implicit timeout: Timeout, parse: Parse[A]) =
    clientRef.ask(RandomKey[A]).mapTo[Option[A]]

  // RENAME (oldkey, newkey)
  // atomically renames the key oldkey to newkey.
  def rename(oldkey: Any, newkey: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Rename(oldkey, newkey)).mapTo[Boolean]

  // RENAMENX (oldkey, newkey)
  // rename oldkey into newkey but fails if the destination key newkey already exists.
  def renamenx(oldkey: Any, newkey: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Rename(oldkey, newkey, nx = true)).mapTo[Boolean]

  // DBSIZE
  // return the size of the db.
  def dbsize()(implicit timeout: Timeout) =
    clientRef.ask(DBSize).mapTo[Long]

  // EXISTS (key)
  // test if the specified key exists.
  def exists(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Exists(key)).mapTo[Boolean]

  // DELETE (key1 key2 ..)
  // deletes the specified keys.
  def del(key: Any, keys: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Del(key, keys:_*)).mapTo[Long]

  // TYPE (key)
  // return the type of the value stored at key in form of a string.
  def getType(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(GetType(key)).mapTo[String]

  // EXPIRE (key, expiry)
  // sets the expire time (in sec.) for the specified key.
  def expire(key: Any, ttl: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Expire(key, ttl)).mapTo[Boolean]

  // PEXPIRE (key, expiry)
  // sets the expire time (in milli sec.) for the specified key.
  def pexpire(key: Any, ttlInMillis: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Expire(key, ttlInMillis, millis = true)).mapTo[Boolean]

  // EXPIREAT (key, unix timestamp)
  // sets the expire time for the specified key.
  def expireat(key: Any, timestamp: Long)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ExpireAt(key, timestamp)).mapTo[Boolean]

  // PEXPIREAT (key, unix timestamp)
  // sets the expire timestamp in millis for the specified key.
  def pexpireat(key: Any, timestampInMillis: Long)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ExpireAt(key, timestampInMillis, millis = true)).mapTo[Boolean]

  // TTL (key)
  // returns the remaining time to live of a key that has a timeout
  def ttl(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(TTL(key)).mapTo[Long]

  // PTTL (key)
  // returns the remaining time to live of a key that has a timeout in millis
  def pttl(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(TTL(key, millis = true)).mapTo[Long]

  // FLUSHDB the DB
  // removes all the DB data.
  def flushdb()(implicit timeout: Timeout) =
    clientRef.ask(FlushDB(all = false)).mapTo[Boolean]

  // FLUSHALL the DB's
  // removes data from all the DB's.
  def flushall()(implicit timeout: Timeout) =
    clientRef.ask(FlushDB(all = true)).mapTo[Boolean]

  // MOVE
  // Move the specified key from the currently selected DB to the specified destination DB.
  def move(key: Any, db: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Move(key, db)).mapTo[Boolean]

  // QUIT
  // exits the server.
  def quit()(implicit timeout: Timeout): Future[Boolean] = {
    clientRef ! Quit
    gracefulStop(clientRef, 5 seconds)
  }

  // AUTH
  // auths with the server.
  def auth(secret: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Auth(secret)).mapTo[Boolean]

  // PERSIST (key)
  // Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
  // to persistent (a key that will never expire as no timeout is associated).
  def persist(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Persist(key)).mapTo[Boolean]
}
