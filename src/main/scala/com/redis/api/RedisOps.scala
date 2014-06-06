package com.redis
package api

import akka.actor.ActorRef


private [redis] trait RedisOps extends StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations
  with HashOperations
  with HyperLogLogOperations
  with KeyOperations
  with ServerOperations
  with EvalOperations
  with ConnectionOperations
  with TransactionOperations {

  def clientRef: ActorRef
}
