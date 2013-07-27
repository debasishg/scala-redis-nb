package com.redis.pipeline

import akka.io._
import akka.util.CompactByteString
import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec
import com.redis.protocol._
import com.redis.serialization.Deserializer
import Deserializer.Result


class Deserializing extends PipelineStage[HasLogging, Command, Command, Event , Event] {
  val parser = new Deserializer()
  val replyAggregator = new ListBuffer[RedisReply[_]]

  def apply(ctx: HasLogging) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    def parse(data: CompactByteString): Iterable[Result] = {
      @tailrec def parseChunk(): Iterable[Result] =
        parser.parse() match {
          case Result.Ok(reply) =>
            replyAggregator += reply
            parseChunk()

          case Result.NeedMoreData =>
            if (replyAggregator.isEmpty) ctx.nothing
            else {
              val replies = replyAggregator.result
              replyAggregator.clear()
              ctx.singleEvent(RedisReplyEvent(replies))
            }

          case Result.Failed(err, data) =>
            log.error(err, "Failed to parse response: {}", data.utf8String.replace("\r\n", "\\r\\n"))
            ctx.singleCommand(Close)
        }

      parser.append(data)
      parseChunk()
    }

    val commandPipeline = (cmd: Command) => ctx.singleCommand(cmd)

    val eventPipeline = (evt: Event) => evt match {
      case Tcp.Received(data: CompactByteString) => parse(data)

      case evt => ctx.singleEvent(evt)
    }

  }
}
