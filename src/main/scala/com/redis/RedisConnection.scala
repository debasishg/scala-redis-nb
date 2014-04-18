package com.redis

import java.net.InetSocketAddress
import akka.actor._
import akka.io.{BackpressureBuffer, IO, Tcp, TcpPipelineHandler}
import java.util.concurrent.TimeUnit
import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.annotation.tailrec
import com.redis.RedisClientSettings.ReconnectionSettings
import pipeline._
import protocol._

object RedisConnection {
  def props(remote: InetSocketAddress, settings: RedisClientSettings) = Props(classOf[RedisConnection], remote, settings)
}

private [redis] class RedisConnection(remote: InetSocketAddress, settings: RedisClientSettings) 
  extends Actor with ActorLogging {
  import Tcp._
  import context.system

  private[this] var pendingRequests = Queue.empty[RedisRequest]
  private[this] var txnRequests = Queue.empty[RedisRequest]
  private[this] var reconnectionSchedule: Option[_ <: ReconnectionSettings#ReconnectionSchedule] = None

  IO(Tcp) ! Connect(remote)

  def receive = unconnected

  def unconnected: Receive = {
    case cmd: RedisCommand[_] =>
      log.info("Attempting to send command before connected: {}", cmd)
      addPendingRequest(cmd)

    case Connected(remote, _) =>
      log.info("Connected to redis server {}:{}.", remote.getHostName, remote.getPort)
      val connection = sender
      val pipe = context.actorOf(TcpPipelineHandler.props(init, connection, self), name = "pipeline")
      connection ! Register(pipe)
      sendAllPendingRequests(pipe)
      context become (running(pipe))
      context watch pipe

    case CommandFailed(c: Connect) =>
      settings.reconnectionSettings match {
        case Some(r) =>
          if (reconnectionSchedule.isEmpty) {
            reconnectionSchedule = Some(settings.reconnectionSettings.get.newSchedule)
          }
          val delayMs = reconnectionSchedule.get.nextDelayMs
          log.error("Connect failed for {} with {}. Reconnecting in {} ms... ", c.remoteAddress, c.failureMessage, delayMs)
          context.system.scheduler.scheduleOnce(Duration(delayMs, TimeUnit.MILLISECONDS), IO(Tcp), Connect(remote))(context.dispatcher, self)
        case None =>
          log.error("Connect failed for {} with {}. Stopping... ", c.remoteAddress, c.failureMessage)
          context stop self
      }
  }

  def transactional(pipe: ActorRef): Receive = withTerminationManagement {
    case TransactionCommands.Exec =>
      sendAllTxnRequests(pipe)
      sendRequest(pipe, RedisRequest(sender, TransactionCommands.Exec))
      context become (running(pipe))

    case TransactionCommands.Discard =>
      txnRequests = txnRequests.drop(txnRequests.size)
      sendRequest(pipe, RedisRequest(sender, TransactionCommands.Discard))
      context become (running(pipe))

    // this should not happen
    // if it comes allow to flow through and Redis will report an error
    case TransactionCommands.Multi =>
      sendRequest(pipe, RedisRequest(sender, TransactionCommands.Multi))

    case cmd: RedisCommand[_] =>
      log.debug("Received a command in Multi: {}", cmd)
      addTxnRequest(cmd)

  }

  def running(pipe: ActorRef): Receive = withTerminationManagement {
    case TransactionCommands.Multi =>
      sendRequest(pipe, RedisRequest(sender, TransactionCommands.Multi))
      context become (transactional(pipe))

    case command: RedisCommand[_] =>
      sendRequest(pipe, RedisRequest(sender, command))

    case init.Event(BackpressureBuffer.HighWatermarkReached) =>
      log.info("Backpressure is too high. Start buffering...")
      context become (buffering(pipe))

    case c: CloseCommand =>
      log.info("Got to close ..")
      sendAllPendingRequests(pipe)
      context become (closing(pipe))
  }

  def buffering(pipe: ActorRef): Receive = withTerminationManagement {
    case cmd: RedisCommand[_] =>
      log.debug("Received a command while buffering: {}", cmd)
      addPendingRequest(cmd)

    case init.Event(BackpressureBuffer.LowWatermarkReached) =>
      log.info("Client backpressure became lower, resuming...")
      context become running(pipe)
      sendAllPendingRequests(pipe)
  }

  def closing(pipe: ActorRef): Receive = withTerminationManagement {
    case init.Event(RequestQueueEmpty) =>
      log.debug("All done.")
      context stop self

    case init.Event(Closed) =>
      log.debug("Closed")
      context stop self
  }

  def withTerminationManagement(handler: Receive): Receive = handler orElse {
    case Terminated(x) => {
      settings.reconnectionSettings match {
        case Some(r) =>
          if (reconnectionSchedule.isEmpty) {
            reconnectionSchedule = Some(settings.reconnectionSettings.get.newSchedule)
          }
          val delayMs = reconnectionSchedule.get.nextDelayMs
          log.error("Child termination detected: {}. Reconnecting in {} ms... ", x, delayMs)
          context become unconnected
          context.system.scheduler.scheduleOnce(Duration(delayMs, TimeUnit.MILLISECONDS), IO(Tcp), Connect(remote))(context.dispatcher, self)
        case None =>
          log.error("Child termination detected: {}", x)
          context stop self
      }
    }
  }

  def addPendingRequest(cmd: RedisCommand[_]): Unit =
    pendingRequests :+= RedisRequest(sender, cmd)

  def addTxnRequest(cmd: RedisCommand[_]): Unit =
    txnRequests :+= RedisRequest(sender, cmd)

  def sendRequest(pipe: ActorRef, req: RedisRequest): Unit = {
    pipe ! init.Command(req)
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

  val init = {
    import RedisClientSettings._
    import settings._

    val stages = Seq(
      Some(new ResponseHandling),
      Some(new Serializing),
      backpressureBufferSettings map {
        case BackpressureBufferSettings(lowBytes, highBytes, maxBytes) =>
          new BackpressureBuffer(lowBytes, highBytes, maxBytes)
      }
    ).flatten.reduceLeft(_ >> _)

    TcpPipelineHandler.withLogger(log, stages)
  }

}
