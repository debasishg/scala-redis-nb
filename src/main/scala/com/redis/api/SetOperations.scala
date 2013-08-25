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
  def sadd(key: String, values: Seq[Stringified])(implicit timeout: Timeout) =
    clientRef.ask(SAdd(key, values)).mapTo[SAdd#Ret]

  def sadd(key: String, value: Stringified, values: Stringified*)(implicit timeout: Timeout) =
    clientRef.ask(SAdd(key, value, values:_*)).mapTo[SAdd#Ret]


  // SREM (VARIADIC: >= 2.4)
  // Remove the specified members from the set value stored at key.
  def srem(key: String, values: Seq[Stringified])(implicit timeout: Timeout) =
    clientRef.ask(SRem(key, values)).mapTo[SRem#Ret]

  def srem(key: String, value: Stringified, values: Stringified*)(implicit timeout: Timeout) =
    clientRef.ask(SRem(key, value, values:_*)).mapTo[SRem#Ret]


  // SPOP
  // Remove and return (pop) a random element from the Set value at key.
  def spop[A](key: String)(implicit timeout: Timeout, reader: Reader[A]) =
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
    clientRef.ask(SIsMember(key, value)).mapTo[SIsMember#Ret]

  // SINTER
  // Return the intersection between the Sets stored at key1, key2, ..., keyN.
  def sinter[A](keys: Seq[String])(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SInter[A](keys)).mapTo[SInter[A]#Ret]

  def sinter[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SInter[A](key, keys:_*)).mapTo[SInter[A]#Ret]

  // SINTERSTORE
  // Compute the intersection between the Sets stored at key1, key2, ..., keyN,
  // and store the resulting Set at dstkey.
  // SINTERSTORE returns the size of the intersection, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sinterstore(destKey: String, keys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(SInterStore(destKey, keys)).mapTo[SInterStore#Ret]

  def sinterstore(destKey: String, key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(SInterStore(destKey, key, keys:_*)).mapTo[SInterStore#Ret]


  // SUNION
  // Return the union between the Sets stored at key1, key2, ..., keyN.
  def sunion[A](keys: Seq[String])(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SUnion[A](keys)).mapTo[SUnion[A]#Ret]

  def sunion[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SUnion[A](key, keys:_*)).mapTo[SUnion[A]#Ret]


  // SUNIONSTORE
  // Compute the union between the Sets stored at key1, key2, ..., keyN,
  // and store the resulting Set at dstkey.
  // SUNIONSTORE returns the size of the union, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sunionstore(destKey: String, keys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(SUnionStore(destKey, keys)).mapTo[SUnionStore#Ret]

  def sunionstore(destKey: String, key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(SUnionStore(destKey, key, keys:_*)).mapTo[SUnionStore#Ret]


  // SDIFF
  // Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
  def sdiff[A](keys: Seq[String])(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SDiff(keys)).mapTo[SDiff[A]#Ret]

  def sdiff[A](key: String, keys: String*)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SDiff(key, keys:_*)).mapTo[SDiff[A]#Ret]


  // SDIFFSTORE
  // Compute the difference between the Set key1 and all the Sets key2, ..., keyN,
  // and store the resulting Set at dstkey.
  def sdiffstore(destKey: String, keys: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(SDiffStore(destKey, keys)).mapTo[SDiffStore#Ret]

  def sdiffstore(destKey: String, key: String, keys: String*)(implicit timeout: Timeout) =
    clientRef.ask(SDiffStore(destKey, key, keys:_*)).mapTo[SDiffStore#Ret]


  // SMEMBERS
  // Return all the members of the Set value at key.
  def smembers[A](key: String)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SMembers(key)).mapTo[SMembers[A]#Ret]

  // SRANDMEMBER
  // Return a random element from a Set
  def srandmember[A](key: String)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SRandMember(key)).mapTo[SRandMember[A]#Ret]

  // SRANDMEMBER
  // Return multiple random elements from a Set (since 2.6)
  def srandmember[A](key: String, count: Int)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(SRandMembers(key, count)).mapTo[SRandMembers[A]#Ret]
}

