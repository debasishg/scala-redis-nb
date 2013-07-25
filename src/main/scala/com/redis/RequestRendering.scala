package com.redis

import akka.io._
import com.redis.command.RedisCommand


class RequestRendering extends PipelineStage[HasLogging, Command, Command, Event, Event] {

  def apply(ctx: HasLogging) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    def render(req: RedisCommand) = {
      ctx.singleCommand(Tcp.Write(req.line))
    }

    val commandPipeline = (cmd: Command) => cmd match {
      case cmd @ RedisRequest(_, redisCmd) => render(redisCmd)
    }

    val eventPipeline = (evt: Event) => ctx.singleEvent(evt)
  }
}
