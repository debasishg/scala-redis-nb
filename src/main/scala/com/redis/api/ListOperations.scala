package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.ListCommands

trait ListOperations { this: RedisOps =>
  import ListCommands._

  // LPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def lpush[A](key: String, value: A, values: A*)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(LPush(key, value, values:_*)).mapTo[LPush[A]#Ret]

  // LPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def lpushx[A](key: String, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(LPushX[A](key, value)).mapTo[LPushX[A]#Ret]

  // RPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def rpush[A](key: String, value: A, values: A*)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(RPush(key, value, values:_*)).mapTo[RPush[A]#Ret]

  // RPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def rpushx[A](key: String, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(RPushX(key, value)).mapTo[RPushX[A]#Ret]

  // LLEN
  // return the length of the list stored at the specified key.
  // If the key does not exist zero is returned (the same behaviour as for empty lists).
  // If the value stored at key is not a list an error is returned.
  def llen(key: String)(implicit timeout: Timeout) =
    clientRef.ask(LLen(key)).mapTo[LLen#Ret]

  // LRANGE
  // return the specified elements of the list stored at the specified key.
  // Start and end are zero-based indexes.
  def lrange[A](key: String, start: Int, end: Int)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(LRange(key, start, end)).mapTo[LRange[A]#Ret]

  // LTRIM
  // Trim an existing list so that it will contain only the specified range of elements specified.
  def ltrim(key: String, start: Int, end: Int)(implicit timeout: Timeout) =
    clientRef.ask(LTrim(key, start, end)).mapTo[LTrim#Ret]

  // LINDEX
  // return the especified element of the list stored at the specified key.
  // Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
  def lindex[A](key: String, index: Int)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(LIndex(key, index)).mapTo[LIndex[A]#Ret]

  // LSET
  // set the list element at index with the new value. Out of range indexes will generate an error
  def lset[A](key: String, index: Int, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(LSet(key, index, value)).mapTo[LSet#Ret]

  // LREM
  // Remove the first count occurrences of the value element from the list.
  def lrem[A](key: String, count: Int, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(LRem(key, count, value)).mapTo[LRem#Ret]

  // LPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def lpop[A](key: String)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(LPop(key)).mapTo[LPop[A]#Ret]

  // RPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def rpop[A](key: String)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(RPop(key)).mapTo[RPop[A]#Ret]

  // RPOPLPUSH
  // Remove the first count occurrences of the value element from the list.
  def rpoplpush[A](srcKey: String, dstKey: String)
                  (implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(RPopLPush(srcKey, dstKey)).mapTo[RPopLPush[A]#Ret]

  def brpoplpush[A](srcKey: String, dstKey: String, timeoutInSeconds: Int)
                   (implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(BRPopLPush(srcKey, dstKey, timeoutInSeconds)).mapTo[BRPopLPush[A]#Ret]

  def blpop[A](timeoutInSeconds: Int, key: String, keys: String*)
                (implicit timeout: Timeout, write: Write[A], read: Read[A]) =
    clientRef.ask(BLPop[A](timeoutInSeconds, key, keys:_*)).mapTo[BLPop[A]#Ret]

  def brpop[A](timeoutInSeconds: Int, key: String, keys: String*)
                (implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(BRPop[A](timeoutInSeconds, key, keys:_*)).mapTo[BRPop[A]#Ret]
}
