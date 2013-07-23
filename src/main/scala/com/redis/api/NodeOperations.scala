package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout

trait NodeOperations { this: RedisOps =>
  import NodeCommands._

  // SAVE
  // save the DB on disk now.
  def save =
    actorRef.ask(Save).mapTo[Boolean]

  // BGSAVE
  // save the DB in the background.
  def bgsave =
    actorRef.ask(Save(true)).mapTo[Boolean]

  // LASTSAVE
  // return the UNIX TIME of the last DB SAVE executed with success.
  def lastsave =
    actorRef.ask(LastSave).mapTo[Long]

  // SHUTDOWN
  // Stop all the clients, save the DB, then quit the server.
  def shutdown =
    actorRef.ask(Shutdown).mapTo[Boolean]

  // BGREWRITEAOF
  def bgrewriteaof =
    actorRef.ask(BGRewriteAOF).mapTo[Boolean]

  // INFO
  // the info command returns different information and statistics about the server.
  def info =
    actorRef.ask(Info).mapTo[Option[String]]

  // MONITOR
  // is a debugging command that outputs the whole sequence of commands received by the Redis server.
  def monitor =
    actorRef.ask(Monitor).mapTo[Boolean]

  // SLAVEOF
  // The SLAVEOF command can change the replication settings of a slave on the fly.
  def slaveof(options: Any) =
    actorRef.ask(SlaveOf(options)).mapTo[Boolean]

  def clientgetname =
    actorRef.ask(ClientGetName).mapTo[Option[String]]

  def clientsetname(name: String) =
    actorRef.ask(ClientSetName(name)).mapTo[Boolean]

  def clientkill(ipPort: String) =
    actorRef.ask(ClientKill(ipPort)).mapTo[Boolean]

  def clientlist =
    actorRef.ask(ClientList).mapTo[Option[String]]

  def configget(param: String) =
    actorRef.ask(ConfigGet(param)).mapTo[Option[String]]

  def configset(param: String, value: Any)(implicit format: Format) =
    actorRef.ask(ConfigSet(param, value)).mapTo[Boolean]

  def configresetstat =
    actorRef.ask(ConfigResetStat).mapTo[Boolean]

  def configrewrite =
    actorRef.ask(ConfigRewrite).mapTo[Boolean]
}
