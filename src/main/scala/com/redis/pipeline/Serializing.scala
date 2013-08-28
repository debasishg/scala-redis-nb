package com.redis.pipeline

import akka.io._
import akka.util.{ByteString, ByteStringBuilder}
import com.redis.protocol._


class Serializing extends PipelineStage[HasLogging, Command, Command, Event, Event] {

  def apply(ctx: HasLogging) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    val b = new ByteStringBuilder

    def render(req: RedisCommand[_]) = {
      def addBulk(bulk: ByteString) = {
        b += Bulk
        b ++= ByteString(bulk.size.toString)
        b ++= Newline
        b ++= bulk
        b ++= Newline
      }

      val args = req.params

      b.clear
      b += Multi
      b ++= ByteString((args.size + 1).toString)
      b ++= Newline
      addBulk(req.cmd)
      args.foreach(arg => addBulk(arg.value))
      b.result
    }

    val commandPipeline = (cmd: Command) => cmd match {
      case RedisRequest(_, redisCmd) => ctx.singleCommand(Tcp.Write(render(redisCmd)))
      case cmd: Tcp.Command => ctx.singleCommand(cmd)
    }

    val eventPipeline = (evt: Event) => ctx.singleEvent(evt)
  }
}
