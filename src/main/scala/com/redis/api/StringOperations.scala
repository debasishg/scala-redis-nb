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
  def get[A](key: String)(implicit timeout: Timeout, parse: Read[A]) =
    clientRef.ask(Get[A](key)).mapTo[Get[A]#Ret]

  // SET KEY (key, value)
  // sets the key with the specified value.
  def set[A](key: String, value: A, exORpx: Option[SetExpiryOption] = None, nxORxx: Option[SetConditionOption] = None)
         (implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(Set[A](key, value, exORpx, nxORxx)).mapTo[Set[A]#Ret]

  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  def getset[A, B](key: String, value: A)(implicit timeout: Timeout, write: Write[A], parse: Read[B]) =
    clientRef.ask(GetSet[A, B](key, value)).mapTo[GetSet[A, B]#Ret]

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  def setnx[A](key: String, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(SetNx[A](key, value)).mapTo[SetNx[A]#Ret]

  // SETEX (key, expiry, value)
  // sets the value for the specified key, with an expiry
  def setex[A](key: String, expiry: Int, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(SetEx[A](key, expiry, value)).mapTo[SetEx[A]#Ret]

  // SETPX (key, expiry, value)
  // sets the value for the specified key, with an expiry in millis
  def psetex[A](key: String, expiryInMillis: Int, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(PSetEx[A](key, expiryInMillis, value)).mapTo[PSetEx[A]#Ret]

  // INCR (key)
  // increments the specified key by 1
  def incr(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Incr(key)).mapTo[Incr#Ret]

  // INCRBY (key, by)
  // increments the specified key by increment
  def incrby(key: String, by: Int)(implicit timeout: Timeout) =
    clientRef.ask(Incr(key, Some(by))).mapTo[Incr#Ret]

  // DECR (key)
  // decrements the specified key by 1
  def decr(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Decr(key)).mapTo[Decr#Ret]

  // DECR (key, by)
  // decrements the specified key by increment
  def decrby(key: String, by: Int)(implicit timeout: Timeout) =
    clientRef.ask(Decr(key, Some(by))).mapTo[Decr#Ret]

  // MGET (key, key, key, ...)
  // get the values of all the specified keys.
  def mget[A](key: String, keys: String*)
    (implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(MGet[A](key, keys:_*)).mapTo[MGet[A]#Ret]

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  def mset[A](kvs: (String, A)*)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(MSet[A](kvs:_*)).mapTo[MSet[A]#Ret]

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  def msetnx[A](kvs: (String, A)*)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(MSetNx[A](kvs:_*)).mapTo[MSetNx[A]#Ret]

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset,
  // for the entire length of value.
  def setrange[A](key: String, offset: Int, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(SetRange[A](key, offset, value)).mapTo[SetRange[A]#Ret]

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets
  // start and end (both are inclusive).
  def getrange[A](key: String, start: Int, end: Int)(implicit timeout: Timeout, parse: Read[A]) =
    clientRef.ask(GetRange[A](key, start, end)).mapTo[GetRange[A]#Ret]

  // STRLEN key
  // gets the length of the value associated with the key
  def strlen(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Strlen(key)).mapTo[Strlen#Ret]

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  def append[A](key: String, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(Append[A](key, value)).mapTo[Append[A]#Ret]

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  def getbit(key: String, offset: Int)(implicit timeout: Timeout) =
    clientRef.ask(GetBit(key, offset)).mapTo[GetBit#Ret]

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  def setbit(key: String, offset: Int, value: Boolean)(implicit timeout: Timeout) =
    clientRef.ask(SetBit(key, offset, value)).mapTo[SetBit#Ret]

  // BITOP op destKey srcKey...
  // Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
  def bitop(op: String, destKey: String, srcKeys: String*)(implicit timeout: Timeout) =
    clientRef.ask(BitOp(op, destKey, srcKeys:_*)).mapTo[BitOp#Ret]

  // BITCOUNT key range
  // Count the number of set bits in the given key within the optional range
  def bitcount(key: String, range: Option[(Int, Int)] = None)(implicit timeout: Timeout) =
    clientRef.ask(BitCount(key, range)).mapTo[BitCount#Ret]
}
