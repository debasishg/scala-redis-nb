package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.StringCommands


trait StringOperations { this: RedisOps =>
  import StringCommands._

  // GET (key)
  // gets the value for the specified key.
  def get[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(Get[A](key)).mapTo[Get[A]#Ret]

  // SET KEY (key, value)
  // sets the key with the specified value.
  def set(key: Any, value: Any, nxORxx: Option[SetConditionOption] = None, exORpx: Option[SetExpiryOption] = None)
         (implicit timeout: Timeout, format: Format) =
    clientRef.ask(Set(key, value, nxORxx, exORpx)).mapTo[Set#Ret]

  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  def getset[A](key: Any, value: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(GetSet[A](key, value)).mapTo[GetSet[A]#Ret]

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  def setnx(key: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(SetNx(key, value)).mapTo[SetNx#Ret]

  // SETEX (key, expiry, value)
  // sets the value for the specified key, with an expiry
  def setex(key: Any, expiry: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(SetEx(key, expiry, value)).mapTo[SetEx#Ret]

  // SETPX (key, expiry, value)
  // sets the value for the specified key, with an expiry in millis
  def psetex(key: Any, expiryInMillis: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(PSetEx(key, expiryInMillis, value)).mapTo[PSetEx#Ret]

  // INCR (key)
  // increments the specified key by 1
  def incr(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Incr(key)).mapTo[Incr#Ret]

  // INCRBY (key, by)
  // increments the specified key by increment
  def incrby(key: Any, by: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Incr(key, Some(by))).mapTo[Incr#Ret]

  // DECR (key)
  // decrements the specified key by 1
  def decr(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Decr(key)).mapTo[Decr#Ret]

  // DECR (key, by)
  // decrements the specified key by increment
  def decrby(key: Any, by: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Decr(key, Some(by))).mapTo[Decr#Ret]

  // MGET (key, key, key, ...)
  // get the values of all the specified keys.
  def mget[K, V](key: K, keys: K*)
    (implicit timeout: Timeout, format: Format, parseV: Parse[V]) =
    clientRef.ask(MGet[K, V](key, keys:_*)).mapTo[MGet[K, V]#Ret]

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  def mset(kvs: (Any, Any)*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(MSet(kvs:_*)).mapTo[MSet#Ret]

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  def msetnx(kvs: (Any, Any)*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(MSetNx(kvs:_*)).mapTo[MSetNx#Ret]

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset,
  // for the entire length of value.
  def setrange(key: Any, offset: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(SetRange(key, offset, value)).mapTo[SetRange#Ret]

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets
  // start and end (both are inclusive).
  def getrange[A](key: Any, start: Int, end: Int)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(GetRange[A](key, start, end)).mapTo[GetRange[A]#Ret]

  // STRLEN key
  // gets the length of the value associated with the key
  def strlen(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Strlen(key)).mapTo[Strlen#Ret]

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  def append(key: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(Append(key, value)).mapTo[Append#Ret]

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  def getbit(key: Any, offset: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(GetBit(key, offset)).mapTo[GetBit#Ret]

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  def setbit(key: Any, offset: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(SetBit(key, offset, value)).mapTo[SetBit#Ret]

  // BITOP op destKey srcKey...
  // Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
  def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(BitOp(op, destKey, srcKeys:_*)).mapTo[BitOp#Ret]

  // BITCOUNT key range
  // Count the number of set bits in the given key within the optional range
  def bitcount(key: Any, range: Option[(Int, Int)] = None)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(BitCount(key, range)).mapTo[BitCount#Ret]
}
