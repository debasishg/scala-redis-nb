package com.redis


package object serialization {

  //  Type alias to avoid importing it in every file
  type ByteString = akka.util.ByteString
  val ByteString = akka.util.ByteString

}
