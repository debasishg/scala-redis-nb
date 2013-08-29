package com.redis
package api

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import akka.pattern.gracefulStop
import com.redis.protocol._


trait ConnectionOperations { this: RedisOps =>
  import ConnectionCommands._

  // QUIT
  // exits the server.
  def quit()(implicit timeout: Timeout): Future[Boolean] = {
    clientRef ! Quit
    gracefulStop(clientRef, 5 seconds)
  }

  // AUTH
  // auths with the server.
  def auth(secret: String)(implicit timeout: Timeout) =
    clientRef.ask(Auth(secret)).mapTo[Auth#Ret]

  // SELECT (index)
  // selects the DB to connect, defaults to 0 (zero).
  def select(index: Int)(implicit timeout: Timeout) =
    clientRef.ask(Select(index)).mapTo[Select#Ret]

  // ECHO (message)
  // echo the given string.
  def echo(message: String)(implicit timeout: Timeout) =
    clientRef.ask(Echo(message)).mapTo[Echo#Ret]

  // PING
  // pings redis
  def ping()(implicit timeout: Timeout) =
    clientRef.ask(Ping).mapTo[Ping.Ret]
}
