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
    clientRef.ask(Save).mapTo[Boolean]

  // BGSAVE
  // save the DB in the background.
  def bgsave()(implicit timeout: Timeout) =
    clientRef.ask(Save(true)).mapTo[Boolean]

  // LASTSAVE
  // return the UNIX TIME of the last DB SAVE executed with success.
  def lastsave()(implicit timeout: Timeout) =
    clientRef.ask(LastSave).mapTo[Long]

  // SHUTDOWN
  // Stop all the clients, save the DB, then quit the server.
  def shutdown()(implicit timeout: Timeout) = clientRef.ask(Shutdown).mapTo[Boolean]

  // BGREWRITEAOF
  def bgrewriteaof()(implicit timeout: Timeout) = clientRef.ask(BGRewriteAOF).mapTo[Boolean]

  // INFO
  // the info protocol returns different information and statistics about the server.
  def info()(implicit timeout: Timeout) = clientRef.ask(Info).mapTo[Option[String]]

  // MONITOR
  // is a debugging protocol that outputs the whole sequence of commands received by the Redis server.
  def monitor()(implicit timeout: Timeout) =
    clientRef.ask(Monitor).mapTo[Boolean]

  // SLAVEOF
  // The SLAVEOF protocol can change the replication settings of a slave on the fly.
  def slaveof(options: Any)(implicit timeout: Timeout) =
    clientRef.ask(SlaveOf(options)).mapTo[Boolean]

  def clientgetname()(implicit timeout: Timeout) =
    clientRef.ask(ClientGetName).mapTo[Option[String]]

  def clientsetname(name: String)(implicit timeout: Timeout) =
    clientRef.ask(ClientSetName(name)).mapTo[Boolean]

  def clientkill(ipPort: String)(implicit timeout: Timeout) =
    clientRef.ask(ClientKill(ipPort)).mapTo[Boolean]

  def clientlist()(implicit timeout: Timeout) =
    clientRef.ask(ClientList).mapTo[Option[String]]

  def configget(param: String)(implicit timeout: Timeout) =
    clientRef.ask(ConfigGet(param)).mapTo[Option[String]]

  def configset(param: String, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ConfigSet(param, value)).mapTo[Boolean]

  def configresetstat()(implicit timeout: Timeout) =
    clientRef.ask(ConfigResetStat).mapTo[Boolean]

  def configrewrite()(implicit timeout: Timeout) =
    clientRef.ask(ConfigRewrite).mapTo[Boolean]
}
