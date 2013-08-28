package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.NodeCommands

trait NodeOperations { this: RedisOps =>
  import NodeCommands._

  // SAVE
  // save the DB on disk now.
  def save()(implicit timeout: Timeout) =
    clientRef.ask(Save).mapTo[Save.Ret]

  // BGSAVE
  // save the DB in the background.
  def bgsave()(implicit timeout: Timeout) =
    clientRef.ask(BgSave).mapTo[BgSave.Ret]

  // LASTSAVE
  // return the UNIX TIME of the last DB SAVE executed with success.
  def lastsave()(implicit timeout: Timeout) =
    clientRef.ask(LastSave).mapTo[LastSave.Ret]

  // SHUTDOWN
  // Stop all the clients, save the DB, then quit the server.
  def shutdown()(implicit timeout: Timeout) = clientRef.ask(Shutdown).mapTo[Shutdown.Ret]

  // BGREWRITEAOF
  def bgrewriteaof()(implicit timeout: Timeout) = clientRef.ask(BGRewriteAOF).mapTo[BGRewriteAOF.Ret]

  // INFO
  // the info command returns different information and statistics about the server.
  def info()(implicit timeout: Timeout) = clientRef.ask(Info).mapTo[Info#Ret]

  // MONITOR
  // is a debugging command that outputs the whole sequence of commands received by the Redis server.
  def monitor()(implicit timeout: Timeout) =
    clientRef.ask(Monitor).mapTo[Monitor.Ret]

  // SLAVEOF
  // The SLAVEOF command can change the replication settings of a slave on the fly.
  def slaveof(node: Option[(String, Int)])(implicit timeout: Timeout) =
    clientRef.ask(SlaveOf(node)).mapTo[SlaveOf#Ret]


  object client {
    import Client._

    def getname()(implicit timeout: Timeout) =
      clientRef.ask(GetName).mapTo[GetName.Ret]

    def setname(name: String)(implicit timeout: Timeout) =
      clientRef.ask(SetName(name)).mapTo[SetName#Ret]

    def kill(ipPort: String)(implicit timeout: Timeout) =
      clientRef.ask(Kill(ipPort)).mapTo[Kill#Ret]

    def list()(implicit timeout: Timeout) =
      clientRef.ask(List).mapTo[List.Ret]
  }


  object config {
    import Config._

    def get[A](param: String)(implicit timeout: Timeout, reader: Reader[A]) =
      clientRef.ask(Get(param)).mapTo[Get[A]#Ret]

    def set(param: String, value: Stringified)(implicit timeout: Timeout) =
      clientRef.ask(Set(param, value)).mapTo[Set#Ret]

    def resetstat()(implicit timeout: Timeout) =
      clientRef.ask(ResetStat).mapTo[ResetStat.Ret]

    def rewrite()(implicit timeout: Timeout) =
      clientRef.ask(Rewrite).mapTo[Rewrite.Ret]
  }

}
