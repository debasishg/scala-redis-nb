package com.redis
package api

import akka.actor.ActorRef


private[redis] class RedisOps(protected val clientRef: ActorRef)
  extends StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations
  with HashOperations
  with KeyOperations
  with ServerOperations
  with EvalOperations
  with ConnectionOperations
  with TransactionOperations
