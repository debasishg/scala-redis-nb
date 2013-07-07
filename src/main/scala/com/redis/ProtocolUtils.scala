package com.redis

import annotation.tailrec

object ProtocolUtils {

  /**
   * Response codes from the Redis server
   */
  val ERR    = '-'
  val OK     = "OK".getBytes("UTF-8")
  val QUEUED = "QUEUED".getBytes("UTF-8")
  val SINGLE = '+'
  val BULK   = '$'
  val MULTI  = '*'
  val INT    = ':'

  val LS     = "\r\n".getBytes("UTF-8")

  val _cr: Byte        = '\r'.toByte
  val _lf: Byte        = '\n'.toByte
  val _single: Byte    = '+'.toByte
  val _int: Byte       = ':'.toByte
  val _bulk: Byte      = '$'.toByte
  val _multi: Byte     = '*'.toByte

  /**
   * splits an Array[Byte] at a pattern of byte sequence.
   */
  def split(bytes: Array[Byte], delim: Seq[Byte] = Seq(_cr, _lf)): List[Array[Byte]] = {

    val matchesNoneOfDelim = (x: Byte) => delim forall (_ != x)

    @tailrec def split_a(bytes: Array[Byte], acc: List[Array[Byte]], inter: Array[Byte]): List[Array[Byte]] = bytes match {
      case Array(cr, lf, rest@_*) if cr == _cr && lf == _lf => split_a(rest.toArray, acc ::: List(inter), Array.empty[Byte])
      case Array(x, rest@_*) if matchesNoneOfDelim(x) => split_a(rest.toArray, acc, inter :+ x)
      case Array() => acc
    }
    split_a(bytes, List.empty[Array[Byte]], Array.empty[Byte])
  }

  /**
   * split an Array[Byte] of multiple replies into a collection of separate replies
   */ 
  def splitReplies(bytes: Array[Byte]): List[Array[Byte]] = {
    def splitReplies_a(bytes: Array[Byte], acc: List[Array[Byte]]): List[Array[Byte]] = bytes match {
      case Array(single, x, y, cr, lf, rest@_*) if single == _single && cr == _cr && lf == _lf => {
        splitReplies_a(rest.toArray, acc ::: List(Array[Byte](_single, x, y, _cr, _lf)))
      }

      case Array(int, rest@_*) if int == _int => {
        val (x, y) = rest.toArray.splitAt(rest.indexOfSlice(List(_cr, _lf)) + 2)
        splitReplies_a(y.toArray, acc ::: List(_int +: x))
      }

      case Array(bulk, rest@_*) if bulk == _bulk => {
        val (l, a) = rest.toArray.splitAt(rest.toArray.indexOfSlice(List(_cr, _lf)) + 2)
        val (x, r) = a.splitAt(new String(l.dropRight(2), "UTF-8").toInt + 2)
        splitReplies_a(r, acc ::: List(Array[Byte](_bulk) ++ l ++ x))
      }

      case Array(multi, rest@_*) if multi == _multi => { 
        val(l, a) = rest.toArray.splitAt(rest.toArray.indexOfSlice(List(_cr, _lf)) + 2)
        val r = splitReplies(a)
        val no = new String(l.dropRight(2), "UTF-8").toInt
        val (tojoin, others) = r.splitAt(no)
        acc ::: List(Array[Byte](_multi) ++ l ++ tojoin.tail.foldLeft(tojoin.head)(_ ++ _)) ::: others
      }
      case Array() => acc
    }
    splitReplies_a(bytes, List.empty[Array[Byte]])
  }
}
