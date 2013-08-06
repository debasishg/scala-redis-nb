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
  def lpush(key: Any, value: Any, values: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LPush(key, value, values:_*)).mapTo[LPush#Ret]

  // LPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def lpushx(key: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LPushX(key, value)).mapTo[LPushX#Ret]

  // RPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def rpush(key: Any, value: Any, values: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(RPush(key, value, values:_*)).mapTo[RPush#Ret]

  // RPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def rpushx(key: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(RPushX(key, value)).mapTo[RPushX#Ret]

  // LLEN
  // return the length of the list stored at the specified key.
  // If the key does not exist zero is returned (the same behaviour as for empty lists).
  // If the value stored at key is not a list an error is returned.
  def llen(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LLen(key)).mapTo[LLen#Ret]

  // LRANGE
  // return the specified elements of the list stored at the specified key.
  // Start and end are zero-based indexes.
  def lrange[A](key: Any, start: Int, end: Int)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(LRange(key, start, end)).mapTo[LRange[A]#Ret]

  // LTRIM
  // Trim an existing list so that it will contain only the specified range of elements specified.
  def ltrim(key: Any, start: Int, end: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LTrim(key, start, end)).mapTo[LTrim#Ret]

  // LINDEX
  // return the especified element of the list stored at the specified key.
  // Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
  def lindex[A](key: Any, index: Int)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(LIndex(key, index)).mapTo[LIndex[A]#Ret]

  // LSET
  // set the list element at index with the new value. Out of range indexes will generate an error
  def lset(key: Any, index: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LSet(key, index, value)).mapTo[LSet#Ret]

  // LREM
  // Remove the first count occurrences of the value element from the list.
  def lrem(key: Any, count: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LRem(key, count, value)).mapTo[LRem#Ret]

  // LPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def lpop[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(LPop(key)).mapTo[LPop[A]#Ret]

  // RPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def rpop[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(RPop(key)).mapTo[RPop[A]#Ret]

  // RPOPLPUSH
  // Remove the first count occurrences of the value element from the list.
  def rpoplpush[A](srcKey: Any, dstKey: Any)
                  (implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(RPopLPush(srcKey, dstKey)).mapTo[RPopLPush[A]#Ret]

  def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)
                   (implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(BRPopLPush(srcKey, dstKey, timeoutInSeconds)).mapTo[BRPopLPush[A]#Ret]

  def blpop[K,V](timeoutInSeconds: Int, key: K, keys: K*)
                (implicit timeout: Timeout, format: Format, parseK: Parse[K], parseV: Parse[V]) =
    clientRef.ask(BLPop[K, V](timeoutInSeconds, key, keys:_*)).mapTo[BLPop[K, V]#Ret]

  def brpop[K,V](timeoutInSeconds: Int, key: K, keys: K*)
                (implicit timeout: Timeout, format: Format, parseK: Parse[K], parseV: Parse[V]) =
    clientRef.ask(BRPop[K, V](timeoutInSeconds, key, keys:_*)).mapTo[BRPop[K, V]#Ret]
}
