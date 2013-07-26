package com.redis.serialization


object Format {
  def apply(f: PartialFunction[Any, Any]): Format = new Format(f)

  implicit val default: Format = new Format(Map.empty)

  def formatDouble(d: Double, inclusive: Boolean = true) =
    (if (inclusive) ("") else ("(")) + {
      if (d.isInfinity) {
        if (d > 0.0) "+inf" else "-inf"
      } else {
        d.toString
      }
    }
}

class Format(val format: PartialFunction[Any, Any]) {
  def apply(in: Any): String =
    (if (format.isDefinedAt(in)) (format(in)) else (in)) match {
      case d: Double => Format.formatDouble(d, true)
      case x => x.toString
    }

  def orElse(that: Format): Format = Format(format orElse that.format)

  def orElse(that: PartialFunction[Any, Any]): Format = Format(format orElse that)
}

