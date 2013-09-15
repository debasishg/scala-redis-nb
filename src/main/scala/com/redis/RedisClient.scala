package com.redis

import java.net.InetSocketAddress
import akka.actor._

// one RedisClient can have multiple RedisConnections to support pooling, clustering etc. Right now we have only
// one RedsiConnection. In future we may offer APIs like RedisClient.single (single connection), RedisClient.pooled
// (connection pool) etc.

object RedisClient {
  
  def apply(host: String, port: Int = 6379, name: String = defaultName,
            settings: RedisClientSettings = RedisClientSettings())(implicit refFactory: ActorRefFactory): RedisClient =
    apply(new InetSocketAddress(host, port), name, settings)
 
  // More independent setting classes might be introduced for client, connection pool or cluster setup,
  // but not sure of actual interface yet  
  def apply(remote: InetSocketAddress, name: String, settings: RedisClientSettings)
           (implicit refFactory: ActorRefFactory): RedisClient =
    new RedisClient(refFactory.actorOf(RedisConnection.props(remote, settings), name = name))
 
  private def defaultName = "redis-client-" + nameSeq.next
  private val nameSeq = Iterator from 0
}
 
class RedisClient(val clientRef: ActorRef) extends api.RedisOps

