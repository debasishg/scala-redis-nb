package com.redis
package api

import scala.concurrent.duration._
import akka.util.Timeout

object RedisOps extends StringOperations with ListOperations {
  val timeout = Timeout(5 seconds)
}
  
