package com.redis.protocol

import akka.util.CompactByteString
import com.redis.serialization._


abstract class RedisCommand[A](_cmd: String)(implicit _des: PartialDeserializer[A]) {
  import RedisCommand._

  type Ret = A

  val cmd = CompactByteString(_cmd)

  def params: Args

  def des = _des
}


object RedisCommand {

  class Args (val values: Seq[Stringified]) extends AnyVal {
    import Stringified._

    def :+(x: Stringified): Args = new Args(values :+ x)
    def :+[A: Writer](x: A): Args = this :+ x.stringify

    def +:(x: Stringified): Args = new Args(x +: values)
    def +:[A: Writer](x: A): Args = x.stringify +: this

    def ++(xs: Seq[Stringified]): Args = new Args(values ++ xs)
    def ++[A: Writer](xs: Seq[A]): Args = this ++ (xs.map(_.stringify))

    def ++:(xs: Seq[Stringified]): Args = new Args(xs ++: values)
    def ++:[A: Writer](xs: Seq[A]): Args = xs.map(_.stringify) ++: this

    def size = values.size

    def foreach(f: Stringified => Unit) = values foreach f
    def map[A](f: Stringified => A) = values map f
  }

  object Args {
    val empty = new Args(Nil)
  }

}

