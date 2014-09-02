package com.redis

import akka.actor.{Props,ActorLogging, Actor}
import akka.routing.Listeners
import com.redis.protocol.PubSubCommands

object PubSubHandler {

  def props = Props[PubSubHandler]

}

class PubSubHandler extends Actor with ActorLogging with Listeners {

  val handleEvents : Receive = {
    case e : PubSubCommands.PushedMessage =>
      log.debug( s"Event received $e" )
      gossip( e )
  }

  override def receive: Receive = listenerManagement orElse handleEvents
}