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
    clientRef.ask(Save).mapTo[Save#Ret]

  // BGSAVE
  // save the DB in the background.
  def bgsave()(implicit timeout: Timeout) =
    clientRef.ask(Save(true)).mapTo[Save#Ret]

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

  def clientgetname()(implicit timeout: Timeout) =
    clientRef.ask(ClientGetName).mapTo[ClientGetName.Ret]

  def clientsetname(name: String)(implicit timeout: Timeout) =
    clientRef.ask(ClientSetName(name)).mapTo[ClientSetName#Ret]

  def clientkill(ipPort: String)(implicit timeout: Timeout) =
    clientRef.ask(ClientKill(ipPort)).mapTo[ClientKill#Ret]

  def clientlist()(implicit timeout: Timeout) =
    clientRef.ask(ClientList).mapTo[ClientList.Ret]

  def configget[A](param: String)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(ConfigGet(param)).mapTo[ConfigGet[A]#Ret]

  def configset(param: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(ConfigSet(param, value)).mapTo[ConfigSet#Ret]

  def configresetstat()(implicit timeout: Timeout) =
    clientRef.ask(ConfigResetStat).mapTo[ConfigResetStat.Ret]

  def configrewrite()(implicit timeout: Timeout) =
    clientRef.ask(ConfigRewrite).mapTo[ConfigRewrite.Ret]
}
