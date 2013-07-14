package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout

trait SetOperations {
  import SetCommands._

  implicit val timeout: Timeout

  // SADD (VARIADIC: >= 2.4)
  // Add the specified members to the set value stored at key.
  def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SOp(add, key, value, values:_*)).mapTo[Long] 
  }

  // SREM (VARIADIC: >= 2.4)
  // Remove the specified members from the set value stored at key.
  def srem(key: Any, value: Any, values: Any*)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SOp(rem, key, value, values:_*)).mapTo[Long] 
  }

  // SPOP
  // Remove and return (pop) a random element from the Set value at key.
  def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(SPop(key)).mapTo[Option[A]] 
  }

  // SMOVE
  // Move the specified member from one Set to another atomically.
  def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SMove(sourceKey, destKey, value)).mapTo[Long] 
  }

  // SCARD
  // Return the number of elements (the cardinality) of the Set at key.
  def scard(key: Any)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SCard(key)).mapTo[Long] 
  }

  // SISMEMBER
  // Test if the specified value is a member of the Set at key.
  def sismember(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(∈(key, value)).mapTo[Boolean] 
  }

  // SINTER
  // Return the intersection between the Sets stored at key1, key2, ..., keyN.
  def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[Set[Option[A]]]] = {client: ActorRef =>
    client.ask(∩∪∖(inter, key, keys:_*)).mapTo[Option[Set[Option[A]]]] 
  }

  // SINTERSTORE
  // Compute the intersection between the Sets stored at key1, key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  // SINTERSTORE returns the size of the intersection, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sinterstore(destKey: Any, key: Any, keys: Any*)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SUXDStore(inter, destKey, key, keys:_*)).mapTo[Long] 
  }

  // SUNION
  // Return the union between the Sets stored at key1, key2, ..., keyN.
  def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[Set[Option[A]]]] = {client: ActorRef =>
    client.ask(∩∪∖(union, key, keys:_*)).mapTo[Option[Set[Option[A]]]] 
  }

  // SUNIONSTORE
  // Compute the union between the Sets stored at key1, key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  // SUNIONSTORE returns the size of the union, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sunionstore(destKey: Any, key: Any, keys: Any*)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SUXDStore(union, destKey, key, keys:_*)).mapTo[Long] 
  }

  // SDIFF
  // Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
  def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[Set[Option[A]]]] = {client: ActorRef =>
    client.ask(∩∪∖(diff, key, keys:_*)).mapTo[Option[Set[Option[A]]]] 
  }

  // SDIFFSTORE
  // Compute the difference between the Set key1 and all the Sets key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  def sdiffstore(destKey: Any, key: Any, keys: Any*)(implicit format: Format): ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(SUXDStore(diff, destKey, key, keys:_*)).mapTo[Long] 
  }

  // SMEMBERS
  // Return all the members of the Set value at key.
  def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[Set[Option[A]]]] = {client: ActorRef =>
    client.ask(SMembers(key)).mapTo[Option[Set[Option[A]]]] 
  }

  // SRANDMEMBER
  // Return a random element from a Set
  def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(SRandMember(key)).mapTo[Option[A]] 
  }

  // SRANDMEMBER
  // Return multiple random elements from a Set (since 2.6)
  def srandmember[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]): ActorRef => Future[List[Option[A]]] = {client: ActorRef =>
    client.ask(SRandMembers(key, count)).mapTo[List[Option[A]]] 
  }
}

