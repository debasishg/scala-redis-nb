package com.redis
package api

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.util.Timeout
import akka.pattern.gracefulStop

private[redis] class RedisOps(protected val actorRef: ActorRef)
  extends StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations
  with HashOperations
  with KeyOperations
  with NodeOperations {

  def close(): Future[Boolean] = {
    actorRef ! RedisClient.Close
    gracefulStop(actorRef, 5 seconds)
  }

  implicit val timeout: Timeout = 5 seconds
}
