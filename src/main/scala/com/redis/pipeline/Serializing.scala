package com.redis.pipeline

import akka.io._
import com.redis.protocol._


class Serializing extends PipelineStage[HasLogging, Command, Command, Event, Event] {

  def apply(ctx: HasLogging) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    def render(req: RedisCommand) = {
      ctx.singleCommand(Tcp.Write(req.line))
    }

    val commandPipeline = (cmd: Command) => cmd match {
      case RedisRequest(_, redisCmd) => render(redisCmd)
      case cmd: Tcp.Command => ctx.singleCommand(cmd)
    }

    val eventPipeline = (evt: Event) => ctx.singleEvent(evt)
  }
}
