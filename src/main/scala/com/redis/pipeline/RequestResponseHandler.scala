package com.redis.pipeline

import java.util
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.{Failure, Success, Try}

import akka.actor.{Status, ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.util.{ByteString, CompactByteString}

import com.redis.protocol._
import com.redis.serialization.Deserializer
import com.redis.serialization.Deserializer.Result


trait RequestResponseHandler {
  def serialize(request: RedisRequest): ByteString
  def deserialize(response: ByteString): immutable.Seq[(ActorRef, Try[Any])]
  def disconnect(): Unit
}
object RequestResponseHandler {
  def apply(implicit actorSystem: ActorSystem): RequestResponseHandler =
    new QueuingRequestResponseHandler(
      new Serializing, new Deserializer, akka.event.Logging.getLogger(actorSystem, this))

  private final class QueuingRequestResponseHandler
      (serializer: (RedisCommand[_] => ByteString), deserializer: Deserializer, log: LoggingAdapter)
      extends RequestResponseHandler {

    private[this] val requestsInFlight: util.Queue[RedisRequest] = new ConcurrentLinkedQueue[RedisRequest]()
    private[this] val transactionRequests: util.Queue[RedisRequest] = new ConcurrentLinkedQueue[RedisRequest]
    private[this] val inTransaction = new AtomicBoolean(false)

    private def isInTransaction(command: RedisCommand[_]): Boolean =
      inTransaction.get() && command != TransactionCommands.Multi && command != TransactionCommands.Exec

    private def handleRequest(request: RedisRequest) = {

      // Multi begins a txn mode & Discard ends a txn mode
      if (request.command == TransactionCommands.Multi) inTransaction.set(true)
      else if (request.command == TransactionCommands.Discard) inTransaction.set(false)

      // queue up all commands between multi & exec
      // this is an optimization that sends commands in bulk for transaction mode
      if (isInTransaction(request.command)) {
        transactionRequests offer request
      }

      log.debug("Sending {}, previous head: {}", request.command, Option(requestsInFlight.peek()).map(_.command))

      requestsInFlight offer request
      serializer(request.command)
    }

    private def handleResponse(response: ByteString) = {
      log.info("Received data from server: {}", response.utf8String.replace("\r\n", "\\r\\n"))
      parseAndDispatch(response.compact, Nil)
    }

    @tailrec
    private def parseExecResponse(data: CompactByteString, soFar: List[(ActorRef, Try[Any])]): immutable.Seq[(ActorRef, Try[Any])] = {
      if (transactionRequests.isEmpty) soFar.reverse
      else {
        val request = transactionRequests.peek()
        deserializer.parse(data, request.command.des) match {
          case Result.Ok(reply, remaining) =>
            val result = reply match {
              case err: RedisError => Failure(err)
              case x => Success(x)
            }
            transactionRequests.poll()
            parseExecResponse(remaining, (request.sender, result) :: soFar)

          case Result.NeedMoreData =>
            if (data.isEmpty) soFar.reverse
            else parseExecResponse(data, soFar)

          case Result.Failed(err, remaining) =>
            log.error(err, "Response parsing failed: {}", data.utf8String.replace("\r\n", "\\r\\n"))
            parseExecResponse(remaining, (request.sender, Failure(err)) :: soFar)
        }
      }
    }

    private def getTransactionResult(data: CompactByteString, request: RedisRequest): immutable.Seq[(ActorRef, Try[Any])] = {
      val singleResults = if (Deserializer.nullMultiBulk(data)) {
        transactionRequests.clear()
        List(request.sender -> Failure(Deserializer.EmptyTxnResultException))
      } else {
        parseExecResponse(data.drop(data.indexOf(com.redis.protocol.Lf) + 1).compact, List())
      }
      val allResults = singleResults map {
        case (_, Success(r)) => r
        case (_, Failure(e)) => akka.actor.Status.Failure(e)
      }
      List(request.sender -> Success(allResults))
    }

    @tailrec
    private def parseAndDispatch(data: CompactByteString, soFar: List[(ActorRef, Try[Any])]): immutable.Seq[(ActorRef, Try[Any])] = {
      if (requestsInFlight.isEmpty || data.isEmpty) soFar.reverse
      else {
        val request = requestsInFlight.peek()
        if (request.command == TransactionCommands.Exec) {
          requestsInFlight.poll()
          inTransaction.set(false)
          if (data.isEmpty) soFar.reverse
          else getTransactionResult(data, request)
        } else {
          deserializer.parse(data, request.command.des) match {
            case Result.Ok(reply, remaining) =>
              requestsInFlight.poll()
              reply match {
                case Queued => parseAndDispatch(remaining, soFar)
                case err: RedisError => parseAndDispatch(remaining, (request.sender, Failure(err)) :: soFar)
                case result => parseAndDispatch(remaining, (request.sender, Success(result)) :: soFar)
              }

            case Result.NeedMoreData => soFar.reverse

            case Result.Failed(err, remaining) =>
              requestsInFlight.poll()
              log.error(err, "Response parsing failed: {}", data.utf8String.replace("\r\n", "\\r\\n"))
              parseAndDispatch(remaining, (request.sender, Failure(err)) :: soFar)
          }
        }
      }
    }


    def serialize(request: RedisRequest): ByteString =
      handleRequest(request)

    def deserialize(response: ByteString): immutable.Seq[(ActorRef, Try[Any])] =
      handleResponse(response)

    def disconnect(): Unit = {
      @tailrec
      def loop(q: Queue[RedisRequest])(action: RedisRequest => Unit): Unit = {
        val head = q.poll()
        if (head ne null) {
          action(head)
          loop(q)(action)
        }
      }
      loop(requestsInFlight) { req =>
        req.sender ! Status.Failure(Disconnected)
      }
      loop(transactionRequests) { req =>
        req.sender ! Status.Failure(Disconnected)
      }
    }
  }
}
