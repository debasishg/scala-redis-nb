package com.redis.pipeline

import akka.io.PipePair


abstract class DynamicPipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow]
    extends PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] {

  type CPL = CmdAbove => Iterable[Result]
  type EPL = EvtBelow => Iterable[Result]

  private[this] var _cpl: CPL = _
  private[this] var _epl: EPL = _

  override def commandPipeline = {
    if (_cpl eq null) _cpl = initialCommandPipeline
    _cpl
  }

  override def eventPipeline = {
    if (_epl eq null) _epl = initialEventPipeline
    _epl
  }

  def initialCommandPipeline: CPL
  def initialEventPipeline: EPL

  protected def become(pipes: SwitchablePipes): Unit = {
    _cpl = pipes.commandPipeline
    _epl = pipes.eventPipeline
  }

  case class SwitchablePipes(commandPipeline: CPL, eventPipeline: EPL)
}
