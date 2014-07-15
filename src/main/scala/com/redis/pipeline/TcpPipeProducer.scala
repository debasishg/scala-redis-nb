package com.redis.pipeline

import akka.actor.{Actor, Props, Stash}
import akka.stream.actor.ActorProducer

import com.redis.protocol.RedisRequest


class TcpPipeProducer() extends Actor with Stash with ActorProducer[RedisRequest] {

  def becomeProducing(): Unit = {
    unstashAll()
    context become producing
  }

  def becomeBackingOff(): Unit = {
    stash()
    context become backingOff
  }

  override def onNext(element: RedisRequest): Unit =
    if (isActive && totalDemand > 0) super.onNext(element)
    else becomeBackingOff()

  def backingOff: Receive = {
    case ActorProducer.Request(_) if isActive => becomeProducing()
    case RedisRequest(_, _)  => stash()
  }

  def producing: Receive = {
    case request: RedisRequest => onNext(request)
  }

  def receive = backingOff
}

object TcpPipeProducer {
  def props() = Props[TcpPipeProducer]
}
