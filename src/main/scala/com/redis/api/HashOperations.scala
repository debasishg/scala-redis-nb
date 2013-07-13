package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import RedisCommand._

trait HashOperations {
  import HashCommands._

  implicit val timeout: Timeout

  def hset(key: Any, field: Any, value: Any)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(HSet(key, field, value)).mapTo[Boolean] 
  }
  
  def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(HSet(key, field, value, true)).mapTo[Boolean] 
  }
  
  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(HGet(key, field)).mapTo[Option[A]] 
  }
  
  def hmset(key: Any, map: Iterable[Product2[Any,Any]])(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(HMSet(key, map)).mapTo[Boolean] 
  }
  
  def hmget[K,V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): ActorRef => Future[Option[Map[K,V]]] = {client: ActorRef =>
    client.ask(HMGet(key, fields:_*)).mapTo[Option[Map[K, V]]] 
  }
  
  def hincrby(key: Any, field: Any, value: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(HIncrby(key, field, value)).mapTo[Option[Long]] 
  }
  
  def hexists(key: Any, field: Any)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(HExists(key, field)).mapTo[Boolean] 
  }
  
  def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(HDel(key, field, fields:_*)).mapTo[Option[Long]] 
  }
  
  def hlen(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(HLen(key)).mapTo[Option[Long]] 
  }
  
  def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>
    client.ask(HKeys(key)).mapTo[Option[List[Option[A]]]] 
  }
  
  def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>
    client.ask(HVals(key)).mapTo[Option[List[Option[A]]]] 
  }
  
  def hgetall[K,V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): ActorRef => Future[Option[Map[K,V]]] = {client: ActorRef =>
    client.ask(HGetall(key)).mapTo[Option[Map[K, V]]]
  }
}
