package com.redis

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.existentials

import akka.actor._
import akka.io.IO
import akka.stream.io.StreamTcp

import com.redis.pipeline.AkkaStreamTransport
import com.redis.protocol.{RedisCommand, RedisRequest, TransactionCommands}


object RedisConnection {
  def props(remote: InetSocketAddress, settings: RedisClientSettings) = Props(classOf[RedisConnection], remote, settings)
}

private [redis] class RedisConnection(remote: InetSocketAddress, settings: RedisClientSettings)
  extends Actor with ActorLogging with AkkaStreamTransport {

  private[this] var pendingRequests = Queue.empty[RedisRequest]
  private[this] var txnRequests = Queue.empty[RedisRequest]
  private[this] var reconnectionSchedule = settings.reconnectionSettings.newSchedule

  private val ioExtension = IO(StreamTcp)(context.system)

  private def reconnectIn(pipe: ActorRef, delay: FiniteDuration): Unit = {
    context unwatch pipe
    context stop pipe
    context become unconnected
    context.system.scheduler.scheduleOnce(delay, ioExtension, connectionMessage(remote))(context.dispatcher, self)
  }

  private def handleDisconnection(pipe: ActorRef, dueToError: Option[Throwable]): Unit = {
    val msg =
      if (reconnectionSchedule.attempts < reconnectionSchedule.maxAttempts) {
        val reconnectDelay = Duration(reconnectionSchedule.nextDelayMs, TimeUnit.MILLISECONDS)
        reconnectIn(pipe, reconnectDelay)
        s"Client disconnected. Reconnecting in $reconnectDelay... "
      } else {
        context stop self
        "Client disconnected."
      }
    dueToError match {
      case Some(ex) => log.error(ex, msg)
      case None => log.info(msg)
    }
  }

  private def becomeRunning(clientBinding: StreamTcp.OutgoingTcpConnection): Unit = {
    val remoteAddress = clientBinding.remoteAddress
    log.info("Stream: Connected to redis server {}:{}", remoteAddress.getHostName, remoteAddress.getPort)

    val (pipe, closeFuture) = initiateConnection(clientBinding.outputStream, clientBinding.inputStream)
    reconnectionSchedule = settings.reconnectionSettings.newSchedule

    closeFuture.onSuccess {
      case optError => handleDisconnection(pipe, optError)
    }(context.dispatcher)

    sendAllPendingRequests(pipe)
    context become running(pipe)
  }

  ioExtension ! connectionMessage(remote)

  def receive = unconnected

  def unconnected: Receive = {
    case cmd: RedisCommand[_] =>
      log.info("Attempting to send command before connected: {}", cmd)
      addPendingRequest(cmd)

    case clientBinding: StreamTcp.OutgoingTcpConnection =>
      becomeRunning(clientBinding)

    case akka.actor.Status.Failure(error) =>
      log.error(error, "Connect failed for {} with {}. Stopping... ", remote, error.getMessage)
      context stop self
  }

  def transactional(pipe: ActorRef): Receive = {
    case TransactionCommands.Exec =>
      sendAllTxnRequests(pipe)
      sendRequest(pipe, RedisRequest(sender(), TransactionCommands.Exec))
      context become running(pipe)

    case TransactionCommands.Discard =>
      txnRequests = txnRequests.drop(txnRequests.size)
      sendRequest(pipe, RedisRequest(sender(), TransactionCommands.Discard))
      context become running(pipe)

    // this should not happen
    // if it comes allow to flow through and Redis will report an error
    case TransactionCommands.Multi =>
      sendRequest(pipe, RedisRequest(sender(), TransactionCommands.Multi))

    case cmd: RedisCommand[_] =>
      log.debug("Received a command in Multi: {}", cmd)
      addTxnRequest(cmd)

  }

  def running(pipe: ActorRef): Receive = {
    case TransactionCommands.Multi =>
      sendRequest(pipe, RedisRequest(sender(), TransactionCommands.Multi))
      context become transactional(pipe)

    case command: RedisCommand[_] =>
      sendRequest(pipe, RedisRequest(sender(), command))
  }

  def buffering(pipe: ActorRef): Receive = {
    case cmd: RedisCommand[_] =>
      log.debug("Received a command while buffering: {}", cmd)
      addPendingRequest(cmd)
  }

  def closing(): Receive = {
    case _ => context stop self
  }

  def addPendingRequest(cmd: RedisCommand[_]): Unit =
    pendingRequests :+= RedisRequest(sender(), cmd)

  def addTxnRequest(cmd: RedisCommand[_]): Unit =
    txnRequests :+= RedisRequest(sender(), cmd)

  def sendRequest(pipe: ActorRef, req: RedisRequest): Unit = {
    pipe ! req
  }

  @tailrec
  final def sendAllPendingRequests(pipe: ActorRef): Unit =
    if (pendingRequests.nonEmpty) {
      sendRequest(pipe, pendingRequests.head)
      pendingRequests = pendingRequests.tail
      sendAllPendingRequests(pipe)
    }

  @tailrec
  final def sendAllTxnRequests(pipe: ActorRef): Unit =
    if (txnRequests.nonEmpty) {
      sendRequest(pipe, txnRequests.head)
      txnRequests = txnRequests.tail
      sendAllTxnRequests(pipe)
    }

  override def unhandled(message: Any): Unit = message match {
    case Terminated(x) =>
      handleDisconnection(x, None)
    case x =>
      super.unhandled(x)
  }
}
