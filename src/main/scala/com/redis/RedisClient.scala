package com.redis

import java.net.InetSocketAddress
import akka.actor._
import akka.io.{BackpressureBuffer, IO, Tcp, TcpPipelineHandler}
import scala.collection.immutable.Queue
import scala.language.existentials
import scala.annotation.tailrec
import pipeline._
import protocol._


object RedisClient {
  import api.RedisOps

  def apply(host: String, port: Int = 6379, name: String = defaultName,
            settings: RedisClientSettings = RedisClientSettings())(implicit refFactory: ActorRefFactory): RedisOps =
    apply(new InetSocketAddress(host, port), name, settings)

  def apply(remote: InetSocketAddress, name: String, settings: RedisClientSettings)
           (implicit refFactory: ActorRefFactory): RedisOps =
    new RedisOps(refFactory.actorOf(Props(classOf[RedisClient], remote, settings), name = name))

  private def defaultName = "redis-client-" + nameSeq.next
  private val nameSeq = Iterator from 0
}

class RedisClient(remote: InetSocketAddress, settings: RedisClientSettings) extends Actor with ActorLogging {
  import Tcp._
  import context.system

  private[this] var pendingRequests = Queue.empty[RedisRequest]

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
      log.error("Connect failed for {} with {}. Stopping... ", c.remoteAddress, c.failureMessage)
      context stop self
  }

  def running(pipe: ActorRef): Receive = withTerminationManagement {
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
      log.info("Child termination detected: {}", x)
      context stop self
    }
  }

  def addPendingRequest(cmd: RedisCommand[_]): Unit =
    pendingRequests :+= RedisRequest(sender, cmd)

  def sendRequest(pipe: ActorRef, req: RedisRequest): Unit =
    pipe ! init.Command(req)

  @tailrec
  final def sendAllPendingRequests(pipe: ActorRef): Unit =
    if (pendingRequests.nonEmpty) {
      sendRequest(pipe, pendingRequests.head)
      pendingRequests = pendingRequests.tail
      sendAllPendingRequests(pipe)
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
