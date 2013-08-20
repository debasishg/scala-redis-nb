package com.redis
package api

import akka.pattern.ask
import akka.util.Timeout
import scala.language.existentials
import protocol.StringCommands
import serialization._


trait StringOperations { this: RedisOps =>
  import StringCommands._

  // GET (key)
  // gets the value for the specified key.
  def get[A](key: String)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(Get[A](key)).mapTo[Get[A]#Ret]

  // SET KEY (key, value)
  // sets the key with the specified value.
  def set(key: String, value: Stringified, exORpx: Option[SetExpiryOption] = None, nxORxx: Option[SetConditionOption] = None)
         (implicit timeout: Timeout) =
    clientRef.ask(Set(key, value, exORpx, nxORxx)).mapTo[Set#Ret]

  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  def getset[A](key: String, value: Stringified)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(GetSet[A](key, value)).mapTo[GetSet[A]#Ret]

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  def setnx(key: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(SetNx(key, value)).mapTo[SetNx#Ret]

  // SETEX (key, expiry, value)
  // sets the value for the specified key, with an expiry
  def setex(key: String, expiry: Int, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(SetEx(key, expiry, value)).mapTo[SetEx#Ret]

  // SETPX (key, expiry, value)
  // sets the value for the specified key, with an expiry in millis
  def psetex(key: String, expiryInMillis: Int, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(PSetEx(key, expiryInMillis, value)).mapTo[PSetEx#Ret]

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
  def mget[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(MGet[A](key, keys:_*)).mapTo[MGet[A]#Ret]

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  def mset(kvs: KeyValuePair*)(implicit timeout: Timeout) =
    clientRef.ask(MSet(kvs:_*)).mapTo[MSet#Ret]

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  def msetnx(kvs: KeyValuePair*)(implicit timeout: Timeout) =
    clientRef.ask(MSetNx(kvs:_*)).mapTo[MSetNx#Ret]

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset,
  // for the entire length of value.
  def setrange(key: String, offset: Int, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(SetRange(key, offset, value)).mapTo[SetRange#Ret]

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets
  // start and end (both are inclusive).
  def getrange[A](key: String, start: Int, end: Int)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(GetRange[A](key, start, end)).mapTo[GetRange[A]#Ret]

  // STRLEN key
  // gets the length of the value associated with the key
  def strlen(key: String)(implicit timeout: Timeout) =
    clientRef.ask(Strlen(key)).mapTo[Strlen#Ret]

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  def append(key: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(Append(key, value)).mapTo[Append#Ret]

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
