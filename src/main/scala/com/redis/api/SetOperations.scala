package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.SetCommands

trait SetOperations { this: RedisOps =>
  import SetCommands._

  // SADD (VARIADIC: >= 2.4)
  // Add the specified members to the set value stored at key.
  def sadd(key: String, value: Stringified, values: Stringified*)(implicit timeout: Timeout) =
    clientRef.ask(SOp(add, key, value, values:_*)).mapTo[SOp#Ret]

  // SREM (VARIADIC: >= 2.4)
  // Remove the specified members from the set value stored at key.
  def srem(key: String, value: Stringified, values: Stringified*)(implicit timeout: Timeout) =
    clientRef.ask(SOp(rem, key, value, values:_*)).mapTo[SOp#Ret]

  // SPOP
  // Remove and return (pop) a random element from the Set value at key.
  def spop[A](key: String)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(SPop[A](key)).mapTo[SPop[A]#Ret]

  // SMOVE
  // Move the specified member from one Set to another atomically.
  def smove(sourceKey: String, destKey: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(SMove(sourceKey, destKey, value)).mapTo[SMove#Ret]

  // SCARD
  // Return the number of elements (the cardinality) of the Set at key.
  def scard(key: String)(implicit timeout: Timeout) =
    clientRef.ask(SCard(key)).mapTo[SCard#Ret]

  // SISMEMBER
  // Test if the specified value is a member of the Set at key.
  def sismember(key: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(∈(key, value)).mapTo[(∈)#Ret]

  // SINTER
  // Return the intersection between the Sets stored at key1, key2, ..., keyN.
  def sinter[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(∩∪∖[A](inter, key, keys:_*)).mapTo[∩∪∖[A]#Ret]

  // SINTERSTORE
  // Compute the intersection between the Sets stored at key1, key2, ..., keyN,
  // and store the resulting Set at dstkey.
  // SINTERSTORE returns the size of the intersection, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sinterstore(destKey: String, key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(SUXDStore(inter, destKey, key, keys:_*)).mapTo[SUXDStore#Ret]

  // SUNION
  // Return the union between the Sets stored at key1, key2, ..., keyN.
  def sunion[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(∩∪∖[A](union, key, keys:_*)).mapTo[∩∪∖[A]#Ret]

  // SUNIONSTORE
  // Compute the union between the Sets stored at key1, key2, ..., keyN,
  // and store the resulting Set at dstkey.
  // SUNIONSTORE returns the size of the union, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sunionstore(destKey: String, key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(SUXDStore(union, destKey, key, keys:_*)).mapTo[SUXDStore#Ret]

  // SDIFF
  // Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
  def sdiff[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(∩∪∖(diff, key, keys:_*)).mapTo[∩∪∖[A]#Ret]

  // SDIFFSTORE
  // Compute the difference between the Set key1 and all the Sets key2, ..., keyN,
  // and store the resulting Set at dstkey.
  def sdiffstore(destKey: String, key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(SUXDStore(diff, destKey, key, keys:_*)).mapTo[SUXDStore#Ret]

  // SMEMBERS
  // Return all the members of the Set value at key.
  def smembers[A](key: String)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(SMembers(key)).mapTo[SMembers[A]#Ret]

  // SRANDMEMBER
  // Return a random element from a Set
  def srandmember[A](key: String)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(SRandMember(key)).mapTo[SRandMember[A]#Ret]

  // SRANDMEMBER
  // Return multiple random elements from a Set (since 2.6)
  def srandmember[A](key: String, count: Int)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(SRandMembers(key, count)).mapTo[SRandMembers[A]#Ret]
}

