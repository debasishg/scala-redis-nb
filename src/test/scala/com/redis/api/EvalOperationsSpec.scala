package com.redis.api

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class EvalOperationsSpec extends RedisSpecBase {

  describe("eval") {
    it("should eval lua code and get a string reply") {
      client.evalBulk[String]("return 'val1';", List(), List()).futureValue should be(Some("val1"))
    }

    it("should eval lua code and get a string array reply") {
      client.evalMultiBulk[String]("return { 'val1','val2' };", List(), List()).futureValue should be(List("val1", "val2"))
    }

    it("should eval lua code and get a string array reply from its arguments") {
      client.evalMultiBulk[String]("return { ARGV[1],ARGV[2] };", List(), List("a", "b")).futureValue should be(List("a", "b"))
    }

    it("should eval lua code and get a string array reply from its arguments & keys") {
      client.set("a", "a")
      client.set("a", "a")
      client.evalMultiBulk[String]("return { KEYS[1],KEYS[2],ARGV[1],ARGV[2] };", List("a", "b"), List("a", "b")).futureValue should be(List("a", "b", "a", "b"))
    }

    it("should eval lua code and get a string reply when passing keys") {
      client.set("a", "b")
      client.evalBulk[String]("return redis.call('get', KEYS[1]);", List("a"), List()).futureValue should be(Some("b"))
    }

    it("should eval lua code and get a string array reply when passing keys") {
      client.lpush("z", "a")
      client.lpush("z", "b")
      client.evalMultiBulk[String]("return redis.call('lrange', KEYS[1], 0, 1);", List("z"), List()).futureValue should be(List("b", "a"))
    }
    
    it("should evalsha lua code hash and execute script when passing keys") {
      val setname = "records";
      
      val luaCode = """
	        local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
	        return res
	        """
      val shahash = client.scriptLoad(luaCode).futureValue
      
      client.zadd(setname, 10, "mmd")
      client.zadd(setname, 22, "mmc")
      client.zadd(setname, 12.5, "mma")
      client.zadd(setname, 14, "mem")
      
      val rs = client.evalMultiSHA[String](shahash.get, List("records"), List()).futureValue
      rs should equal (List("mmd", "10", "mma", "12.5", "mem", "14", "mmc", "22"))
    }
    
    it("should check if script exists when passing its sha hash code") {      
      val luaCode = """
	        local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
	        return res
	        """
      val shahash = client.scriptLoad(luaCode).futureValue
      
      import com.redis.serialization.Parse.Implicits._
      val rs = client.scriptExists(shahash.get).futureValue
      rs should equal (List(1))
    }
    
    it("should remove script cache") {      
      val luaCode = """
	        local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
	        return res
	        """
      val shahash = client.scriptLoad(luaCode).futureValue
      
      client.scriptFlush.futureValue should equal (Some("OK"))
      
      client.scriptExists(shahash.get).futureValue should equal (List(0))
    }
  }
}
