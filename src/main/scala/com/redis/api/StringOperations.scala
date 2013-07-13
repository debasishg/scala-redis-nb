package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout

trait StringOperations {

  import StringCommands._

  implicit val timeout: Timeout

  // GET (key)
  // gets the value for the specified key.
  def get[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(Get[A](key)).mapTo[Option[A]] 
  }

  // SET KEY (key, value)
  // sets the key with the specified value.
  def set(key: Any, value: Any, nxORxx: Option[SetConditionOption] = None, exORpx: Option[SetExpiryOption] = None)
    (implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(Set(key, value, nxORxx, exORpx)).mapTo[Boolean] 
  }
  
  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  def getset[A](key: Any, value: Any)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(GetSet[A](key, value)).mapTo[Option[A]] 
  }

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  def setnx(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(SetNx(key, value)).mapTo[Boolean] 
  }

  // SETEX (key, expiry, value)
  // sets the value for the specified key, with an expiry
  def setex(key: Any, expiry: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(SetEx(key, expiry, value)).mapTo[Boolean] 
  }

  // SETPX (key, expiry, value)
  // sets the value for the specified key, with an expiry in millis
  def psetex(key: Any, expiryInMillis: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(PSetEx(key, expiryInMillis, value)).mapTo[Boolean] 
  }

  // INCR (key)
  // increments the specified key by 1
  def incr(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Incr(key)).mapTo[Option[Long]] 
  }

  // INCRBY (key, by)
  // increments the specified key by increment
  def incrby(key: Any, by: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Incr(key, Some(by))).mapTo[Option[Long]] 
  }

  // DECR (key)
  // decrements the specified key by 1
  def decr(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Decr(key)).mapTo[Option[Long]] 
  }

  // DECR (key, by)
  // decrements the specified key by increment
  def decrby(key: Any, by: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Decr(key, Some(by))).mapTo[Option[Long]] 
  }

  // MGET (key, key, key, ...)
  // get the values of all the specified keys.
  def mget[A](key: Any, keys: Any*)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>
    client.ask(MGet[A](key, keys:_*)).mapTo[Option[List[Option[A]]]] 
  }

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  def mset(kvs: (Any, Any)*)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(MSet(kvs:_*)).mapTo[Boolean]
  }

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  def msetnx(kvs: (Any, Any)*)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(MSetNx(kvs:_*)).mapTo[Boolean]
  }

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset, 
  // for the entire length of value.
  def setrange(key: Any, offset: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(SetRange(key, offset, value)).mapTo[Option[Long]] 
  }

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets 
  // start and end (both are inclusive).
  def getrange[A](key: Any, start: Int, end: Int)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(GetRange[A](key, start, end)).mapTo[Option[A]] 
  }

  // STRLEN key
  // gets the length of the value associated with the key
  def strlen(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Strlen(key)).mapTo[Option[Long]] 
  }

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  def append(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Append(key, value)).mapTo[Option[Long]] 
  }

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  def getbit(key: Any, offset: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(GetBit(key, offset)).mapTo[Option[Long]] 
  }

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  def setbit(key: Any, offset: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(SetBit(key, offset, value)).mapTo[Option[Long]] 
  }

  // BITOP op destKey srcKey...
  // Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
  def bitop(op: String, destKey: Any, srcKeys: Any*)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(BitOp(op, destKey, srcKeys:_*)).mapTo[Option[Long]] 
  }

  // BITCOUNT key range
  // Count the number of set bits in the given key within the optional range
  def bitcount(key: Any, range: Option[(Int, Int)] = None)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(BitCount(key, range)).mapTo[Option[Long]] 
  }
}
