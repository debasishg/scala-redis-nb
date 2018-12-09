package com.redis.pipeline

import akka.io._
import akka.io.TcpPipelineHandler.WithinActorContext
import akka.actor.ActorRef
import akka.actor.Status.Failure
import akka.util.CompactByteString
import com.redis.protocol._
import com.redis.serialization.Deserializer
import com.redis.serialization.Deserializer.Result
import scala.collection.immutable.Queue
import scala.annotation.tailrec
import scala.language.existentials


class ResponseHandling extends PipelineStage[WithinActorContext, Command, Command, Event, Event] {

  def apply(ctx: WithinActorContext) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    private[this] val parser = new Deserializer()
    private[this] val redisClientRef: ActorRef = ctx.getContext.self
    private[this] var sentRequests = Queue.empty[RedisRequest]
    private[this] var txnMode = false
    private[this] var txnRequests = Queue.empty[RedisRequest]

    type ResponseHandler = CompactByteString => Iterable[Result]

    def pubSubHandler(handlerActor: ActorRef) : ResponseHandler = data => {
      def parsePushedEvents(data: CompactByteString) : Iterable[Result] = {
        import com.redis.serialization.PartialDeserializer.pubSubMessagePD
        parser.parse(data, pubSubMessagePD) match {
          case Result.Ok(reply, remaining) =>
            val result = reply match {
              case err: RedisError => Failure(err)
              case _ => reply
            }
            handlerActor ! result
            parsePushedEvents( remaining )
          case Result.NeedMoreData =>
            ctx.nothing
          case Result.Failed(err, data) =>
            log.error(err, "Response parsing failed: {}", data.utf8String.replace("\r\n", "\\r\\n"))
            ctx.singleCommand(Close)
        }
      }
      if (sentRequests.isEmpty) {
        log.debug("Received data from server: {}", data.utf8String.replace("\r\n", "\\r\\n"))
        parsePushedEvents(data)
      }
      else defaultHandler(data)
    }

    val defaultHandler : CompactByteString => Iterable[Result] = {

      @tailrec
      def parseExecResponse(data: CompactByteString, acc: Iterable[Any]): Iterable[Any] = {
        if (txnRequests.isEmpty) acc
        else {
          val (RedisRequest(commander, command), tail) = txnRequests.dequeue
          // process every response with the appropriate de-serializer that we have accumulated in txnRequests
          parser.parse(data, command.des) match {
            case Result.Ok(reply, remaining) =>
              val result = reply match {
                case err: RedisError => Failure(err)
                case _ => reply
              }
              commander.tell(result, redisClientRef)
              txnRequests = tail
              parseExecResponse(remaining, acc ++ List(result))

            case Result.NeedMoreData =>
              if (data isEmpty) ctx.singleEvent(RequestQueueEmpty)
              else parseExecResponse(data, acc)

            case Result.Failed(err, data) =>
              log.error(err, "Response parsing failed: {}", data.utf8String.replace("\r\n", "\\r\\n"))
              ctx.singleCommand(Close)
          }
        }
      }

      (data: CompactByteString) => {
        @tailrec
        def parseAndDispatch(data: CompactByteString): Iterable[Result] =
          if (sentRequests.isEmpty) ctx.singleEvent(RequestQueueEmpty)
          else {
            val (RedisRequest(commander, cmd), tail) = sentRequests.dequeue

            // we have an Exec : need to parse the response which will be a collection of
            // MultiBulk and then end transaction mode
            if (cmd == TransactionCommands.Exec) {
              if (data isEmpty) ctx.singleEvent(RequestQueueEmpty)
              else {
                val result =
                  if (Deserializer.nullMultiBulk(data)) {
                    txnRequests = Queue.empty
                    Failure(Deserializer.EmptyTxnResultException)
                  } else parseExecResponse(data.splitAt(data.indexOf(Lf) + 1)._2.compact, List.empty[Result])

                commander.tell(result, redisClientRef)
                txnMode = false
              }
            }
            parser.parse(data, cmd.des) match {
              case Result.Ok(reply, remaining) =>
                val result = reply match {
                  case err: RedisError => Failure(err)
                  case _ => reply
                }
                log.debug("RESULT: {}", result)
                if (reply != Queued) commander.tell(result, redisClientRef)
                sentRequests = tail
                parseAndDispatch(remaining)

              case Result.NeedMoreData => ctx.singleEvent(RequestQueueEmpty)

              case Result.Failed(err, data) =>
                log.error(err, "Response parsing failed: {}", data.utf8String.replace("\r\n", "\\r\\n"))
                commander.tell(Failure(err), redisClientRef)
                ctx.singleCommand(Close)
            }
          }

        log.debug("Received data from server: {}", data.utf8String.replace("\r\n", "\\r\\n"))
        parseAndDispatch(data)
      }
    }

    private[this] var handleResponse : ResponseHandler = defaultHandler


    val commandPipeline = (cmd: Command) => cmd match {
      case req @ RedisRequest(commander, cmd ) if cmd.isInstanceOf[PubSubCommands.PubSubCommand] =>
        handleResponse = pubSubHandler( commander )
        ctx singleCommand req
      case req: RedisRequest =>

        // Multi begins a txn mode & Discard ends a txn mode
        if (req.command == TransactionCommands.Multi) txnMode = true
        else if (req.command == TransactionCommands.Discard) txnMode = false

        // queue up all commands between multi & exec
        // this is an optimization that sends commands in bulk for transaction mode
        if (txnMode && req.command != TransactionCommands.Multi && req.command != TransactionCommands.Exec) {
          txnRequests = txnRequests.enqueue(req)
        }

        log.debug("Sending {}, previous head: {}", req.command, sentRequests.headOption.map(_.command))
        sentRequests = sentRequests.enqueue(req)
        ctx singleCommand req

      case _ => ctx singleCommand cmd
    }

    val eventPipeline = (evt: Event) => evt match {

      case Tcp.Received(data: CompactByteString) => 
        // println("Received data from server: {}", data.utf8String.replace("\r\n", "\\r\\n"))
        handleResponse(data)

      case _ => ctx singleEvent evt
    }
  }
}
