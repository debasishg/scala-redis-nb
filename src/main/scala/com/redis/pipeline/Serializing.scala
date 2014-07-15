package com.redis.pipeline


import akka.util.{ByteStringBuilder, ByteString}
import com.redis.protocol._

class Serializing extends (RedisCommand[_] => ByteString) {
  val b = new ByteStringBuilder

  def render(req: RedisCommand[_]) = {
    def addBulk(bulk: ByteString) = {
      b += Bulk
      b ++= ByteString(bulk.size.toString)
      b ++= Newline
      b ++= bulk
      b ++= Newline
    }

    val args = req.params

    b.clear()
    b += Multi
    b ++= ByteString((args.size + 1).toString)
    b ++= Newline
    addBulk(req.cmd)
    args.foreach(arg => addBulk(arg.value))
    b.result()
  }

  def apply(req: RedisCommand[_]): ByteString = render(req)
}
