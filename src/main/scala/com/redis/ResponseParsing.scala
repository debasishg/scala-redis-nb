package com.redis

import akka.io._
import akka.util.CompactByteString
import scala.collection.immutable.Queue
import scala.annotation.tailrec
import com.redis.command.RedisCommand
import ResponseParser.ParseResult
import scala.collection.mutable.ListBuffer


class ResponseParsing extends PipelineStage[HasLogging, Command, Command, Event , Event] {
  val parser = new ResponseParser()
  val replyAggregator = new ListBuffer[RedisReply[_]]

  def apply(ctx: HasLogging) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    def parse(data: CompactByteString): Iterable[Result] = {

      @tailrec def inner(input: CompactByteString = CompactByteString.empty): Iterable[Result] =
        parser.parse(input) match {
          case ParseResult.Ok(reply) =>
            log.debug("Parsed reply: {}", reply)
            replyAggregator += reply
            inner(CompactByteString.empty)

          case ParseResult.NeedMoreData =>
            if (replyAggregator.isEmpty) ctx.nothing
            else {
              val replies = replyAggregator.result
              replyAggregator.clear()
              ctx.singleEvent(RedisReplyEvent(replies))
            }

          case ParseResult.Failed(_, data) =>
            log.error("Failed to parse response: {}", data)
            ctx.singleCommand(Close)
        }
      inner(data)
    }

    val commandPipeline = (cmd: Command) => ctx.singleCommand(cmd)

    val eventPipeline = (evt: Event) => evt match {
      case Tcp.Received(data: CompactByteString) => parse(data)

      case evt => ctx.singleEvent(evt)
    }

  }
}
