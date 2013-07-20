package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout

trait NodeOperations {
  import NodeCommands._

  implicit val timeout: Timeout

  // SAVE
  // save the DB on disk now.
  def save: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(Save).mapTo[Boolean] 
  }

  // BGSAVE
  // save the DB in the background.
  def bgsave: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(Save(true)).mapTo[Boolean] 
  }

  // LASTSAVE
  // return the UNIX TIME of the last DB SAVE executed with success.
  def lastsave: ActorRef => Future[Long] = {client: ActorRef =>
    client.ask(LastSave).mapTo[Long] 
  }
  
  // SHUTDOWN
  // Stop all the clients, save the DB, then quit the server.
  def shutdown: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(Shutdown).mapTo[Boolean] 
  }

  // BGREWRITEAOF
  def bgrewriteaof: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(BGRewriteAOF).mapTo[Boolean] 
  }

  // INFO
  // the info command returns different information and statistics about the server.
  def info: ActorRef => Future[Option[String]] = {client: ActorRef =>
    client.ask(Info).mapTo[Option[String]] 
  }
  
  // MONITOR
  // is a debugging command that outputs the whole sequence of commands received by the Redis server.
  def monitor: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(Monitor).mapTo[Boolean] 
  }
  
  // SLAVEOF
  // The SLAVEOF command can change the replication settings of a slave on the fly.
  def slaveof(options: Any): ActorRef => Future[Boolean] = {client: ActorRef => 
    client.ask(SlaveOf(options)).mapTo[Boolean] 
  }

  def clientgetname: ActorRef => Future[Option[String]] = {client: ActorRef =>
    client.ask(ClientGetName).mapTo[Option[String]] 
  }

  def clientsetname(name: String): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(ClientSetName(name)).mapTo[Boolean] 
  }

  def clientkill(ipPort: String): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(ClientKill(ipPort)).mapTo[Boolean] 
  }

  def clientlist: ActorRef => Future[Option[String]] = {client: ActorRef =>
    client.ask(ClientList).mapTo[Option[String]] 
  }

  def configget(param: String): ActorRef => Future[Option[String]] = {client: ActorRef =>
    client.ask(ConfigGet(param)).mapTo[Option[String]] 
  }

  def configset(param: String, value: Any)(implicit format: Format): ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(ConfigSet(param, value)).mapTo[Boolean] 
  }

  def configresetstat: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(ConfigResetStat).mapTo[Boolean] 
  }

  def configrewrite: ActorRef => Future[Boolean] = {client: ActorRef =>
    client.ask(ConfigRewrite).mapTo[Boolean] 
  }
}
