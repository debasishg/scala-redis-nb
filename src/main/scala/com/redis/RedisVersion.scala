package com.redis

object RedisVersion {
  private val RegexFromInfo = """redis_version:(\d+)\.(\d+)\.(\d+)""".r
  private val SemVerRegex = """(\d+)\.(\d+)\.(\d+)""".r
  def apply(major: Int, minor: Int, mini: Int): RedisVersion =
    new RedisVersion(major, minor, mini)
  def unapply(version: String): Option[(Int, Int, Int)] =
    version match {
      case RegexFromInfo(major, minor, mini) => Some((major.toInt, minor.toInt, mini.toInt))
      case SemVerRegex(major, minor, mini)   => Some((major.toInt, minor.toInt, mini.toInt))
      case _ => None
    }
  def compare(v1: RedisVersion, v2: RedisVersion): Int = {
    val intOrdering = implicitly[Ordering[Int]]
    val byMajor = intOrdering.compare(v1.major, v2.major)
    if (byMajor == 0) {
      val byMinor = intOrdering.compare(v1.minor, v2.minor)
      if (byMinor == 0) {
        intOrdering.compare(v1.mini, v2.mini)
      } else byMinor
    } else byMajor
  }
}
class RedisVersion(val major: Int, val minor: Int, val mini: Int) extends Ordered[RedisVersion] {
  def compare(that: RedisVersion): Int =
    RedisVersion.compare(this, that)
  override def toString: String =
    s"v$major.$minor.$mini"
}
