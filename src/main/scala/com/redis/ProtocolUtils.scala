package com.redis

import annotation.tailrec

object ProtocolUtils {

  /**
   * Response codes from the Redis server
   */
  val ERR                 = '-'
  val OK                  = "OK".getBytes("UTF-8")
  val QUEUED              = "QUEUED".getBytes("UTF-8")
  val Array(minus, one)   = "-1".getBytes("UTF-8")
  val SINGLE              = '+'
  val BULK                = '$'
  val MULTI               = '*'
  val INT                 = ':'

  val LS               = "\r\n".getBytes("UTF-8")

  val _cr: Byte        = '\r'.toByte
  val _lf: Byte        = '\n'.toByte
  val _single: Byte    = '+'.toByte
  val _int: Byte       = ':'.toByte
  val _bulk: Byte      = '$'.toByte
  val _multi: Byte     = '*'.toByte
  val _err: Byte       = '-'.toByte

  val NULL_BULK_REPLY_COUNT = -1

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
   * @todo : need to ahndle error replies
   */ 
  def splitReplies(bytes: Array[Byte]): List[Array[Byte]] = {
    def splitReplies_a(bytes: Array[Byte], acc: List[Array[Byte]]): List[Array[Byte]] = bytes match {
      // handle sequence beginning with status reply
      case Array(single, x, y, cr, lf, rest@_*) if single == _single && cr == _cr && lf == _lf => {
        splitReplies_a(rest.toArray, acc ::: List(Array[Byte](_single, x, y, _cr, _lf)))
      }

      // handle sequence beginning with error reply
      case Array(err, rest@_*) if err == _err => {
        val (x, y) = rest.toArray.splitAt(rest.indexOfSlice(List(_cr, _lf)) + 2)
        splitReplies_a(y.toArray, acc ::: List(_err +: x))
      }

      // handle sequence beginning with integer reply
      case Array(int, rest@_*) if int == _int => {
        val (x, y) = rest.toArray.splitAt(rest.indexOfSlice(List(_cr, _lf)) + 2)
        splitReplies_a(y.toArray, acc ::: List(_int +: x))
      }

      // handle sequence beginning with bulk reply

      /**
       * Bulk Reply:
       *
       * C: GET mykey
       * S: $6\r\nfoobar\r\n
       *
       * Can be null - null bulk reply contains (-1) as the data length count
       * C: GET nonexistingkey
       * S: $-1
       */
      case Array(bulk, rest@_*) if bulk == _bulk => {
        val (l, a) = rest.toArray.splitAt(rest.toArray.indexOfSlice(List(_cr, _lf)) + 2)
        val bulkCount = new String(l.dropRight(2), "UTF-8").toInt

        if (bulkCount == NULL_BULK_REPLY_COUNT) {
          splitReplies_a(a, acc ::: List(Array[Byte](_bulk) ++ l))
        } else {
          val (x, r) = a.splitAt(bulkCount + 2)
          splitReplies_a(r, acc ::: List(Array[Byte](_bulk) ++ l ++ x))
        }
      }

      // handle sequence beginning with multi-bulk reply

      /**
       * Multi-Bulk Reply:
       *
       * C: LRANGE mylist 0 3
       * S: *4
       * S: $3
       * S: foo
       * S: $3
       * S: bar
       * S: $5
       * S: Hello
       * S: $5
       * S: World
       *
       * Each bulk reply within the multi-bulk can be a null bulk as well. This should be
       * fine and handled by the bulk reply pattern match.
       *
       * S: *3
       * S: $3
       * S: foo
       * S: $-1
       * S: $3
       * S: bar
       */
      case Array(multi, rest@_*) if multi == _multi => { 
        val(l, a) = rest.toArray.splitAt(rest.toArray.indexOfSlice(List(_cr, _lf)) + 2)
        val r = splitReplies(a)
        val no = new String(l.dropRight(2), "UTF-8").toInt
        val (tojoin, others) = r.splitAt(no)
        acc ::: List(Array[Byte](_multi) ++ l ++ tojoin.foldLeft(Array[Byte]())(_ ++ _)) ::: others
      }
      case Array() => acc
    }
    splitReplies_a(bytes, List.empty[Array[Byte]])
  }
}
