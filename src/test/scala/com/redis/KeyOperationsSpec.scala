package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._
import KeyCommands._


@RunWith(classOf[JUnitRunner])
class KeyOperationsSpec extends RedisSpecBase {

  describe("keys") {
  }

}


