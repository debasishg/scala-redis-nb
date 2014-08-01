package com.redis.serialization

import akka.util.{ByteString, CompactByteString}
import scala.annotation.implicitNotFound
import scala.language.implicitConversions


@implicitNotFound(msg = "Cannot find implicit Read or Format type class for ${A}")
private[redis] trait Reader[A] { self =>
  def fromByteString(in: ByteString): A

  def map[B](f: A => B): Reader[B] =
    new Reader[B] {
      def fromByteString(in: ByteString) =
        f(self.fromByteString(in))
    }
}

private[redis] trait ReaderLowPriorityImplicits {
  implicit object bypassingReader extends Reader[ByteString] {
    def fromByteString(in: ByteString) = in
  }

  implicit object byteArrayReader extends Reader[Array[Byte]] {
    def fromByteString(in: ByteString) = in.toArray[Byte]
  }
}

object Reader extends ReaderLowPriorityImplicits {
  implicit def default: Reader[String] = DefaultFormats.stringFormat
}


@implicitNotFound(msg = "Cannot find implicit Write or Format type class for ${A}")
private[redis] trait Writer[A] { self =>
  private[redis] def toByteString(in: A): ByteString

  def contramap[B](f: B => A): Writer[B] =
    new Writer[B] {
      def toByteString(in: B) =
        self.toByteString(f(in))
    }
}

private[redis] trait WriterLowPriorityImplicits {
  implicit object bypassingWriter extends Writer[ByteString] {
    def toByteString(in: ByteString) = in
  }

  implicit object byteArrayWriter extends Writer[Array[Byte]] {
    def toByteString(in: Array[Byte]) =  CompactByteString(in)
  }
}

object Writer extends WriterLowPriorityImplicits {
  implicit def default: Writer[String] = DefaultFormats.stringFormat
}



trait StringReader[A] extends Reader[A] { self =>
  def read(in: String): A

  def fromByteString(in: ByteString): A = read(in.utf8String)

  override def map[B](f: A => B): StringReader[B] =
    new StringReader[B] {
      def read(in: String) =
        f(self.read(in))
    }
}

object StringReader {
  def apply[A](f: String => A) = new StringReader[A] { def read(in: String) = f(in) }
}

trait DefaultReaders {
  import java.{lang => J}
  implicit val intReader    = StringReader[Int]   (J.Integer.parseInt)
  implicit val shortReader  = StringReader[Short] (J.Short.parseShort)
  implicit val longReader   = StringReader[Long]  (J.Long.parseLong)
  implicit val floatReader  = StringReader[Float] (J.Float.parseFloat)
  implicit val doubleReader = StringReader[Double](J.Double.parseDouble)
  implicit val anyReader    = StringReader[Any]   (identity)
}
object DefaultReaders extends DefaultReaders


trait StringWriter[A] extends Writer[A] { self =>
  def write(in: A): String

  def toByteString(in: A): ByteString = ByteString(write(in))

  override def contramap[B](f: B => A): StringWriter[B] =
    new StringWriter[B] {
      def write(in: B) =
        self.write(f(in))
    }
}

object StringWriter {
  def apply[A](f: A => String) = new StringWriter[A] { def write(in: A) = f(in) }
}

trait DefaultWriters {
  implicit val intWriter    = StringWriter[Int]   (_.toString)
  implicit val shortWriter  = StringWriter[Short] (_.toString)
  implicit val longWriter   = StringWriter[Long]  (_.toString)
  implicit val floatWriter  = StringWriter[Float] (_.toString)
  implicit val doubleWriter = StringWriter[Double](_.toString)
  implicit val anyWriter    = StringWriter[Any]   (_.toString)
}
object DefaultWriters extends DefaultWriters


trait Format[A] extends StringReader[A] with StringWriter[A]

object Format {

  def apply[A](_read: String => A, _write: A => String) = new Format[A] {
    def read(str: String) = _read(str)

    def write(obj: A) = _write(obj)
  }

  implicit def default = DefaultFormats.stringFormat
}


private[serialization] trait LowPriorityFormats extends DefaultReaders with DefaultWriters

trait DefaultFormats extends LowPriorityFormats {
  implicit val stringFormat = Format[String](identity, identity)
}

object DefaultFormats extends DefaultFormats
