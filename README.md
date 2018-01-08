## redisreact [![Build Status](https://travis-ci.org/debasishg/scala-redis-nb.png?branch=master)](https://travis-ci.org/debasishg/scala-redis-nb)

A non blocking Redis client based on Akka I/O

### Key features of the library

- [Non blocking, compositional with Futures](#non-blocking-compositional-with-futures)
- [Full set of Redis commands](#full-set-of-redis-commands)
- [Scripting support](#scripting-support)
- [Transparent typeclass based serialization](#typeclass-based-transparent-serialization)
- [Out of the box integration support with Json serialization libraries](#integration-support-with-json-serializing-libraries)
- [Transaction Support](#transaction-support)

### Project Dependencies

```scala
libraryDependencies ++= Seq(
  "net.debasishg" %% "redisreact" % "0.9"
)
```

### Coming up

- Publish-Subscribe
- Clustering

### Sample usage

```scala
// Akka setup
implicit val system = ActorSystem("redis-client")
implicit val executionContext = system.dispatcher
implicit val timeout = Timeout(5 seconds)

// Redis client setup
val client = RedisClient("localhost", 6379)
```

**Note**

The below examples are taken from the test cases.

Every API call returns Scala [`Future`](http://www.scala-lang.org/api/current/index.html#scala.concurrent.Future) and `_.futureValue` is just a helper provided by [ScalaTest](http://scalatest.org) for ease of test. You won't need this in most cases because it blocks current thread.


#### Non Blocking, Compositional with Futures

```scala
describe("set") {
  it("should set values to keys") {
    val numKeys = 3
    val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
    val writes = keys zip values map { case (key, value) => client.set(key, value) }

    Future.sequence(writes).futureValue should contain only (true)
  }
}
```

```scala
describe("get") {
  it("should get results for keys set earlier") {
    val numKeys = 3
    val (keys, values) = (1 to numKeys map { num => ("get" + num, "value" + num) }).unzip
    val writes = keys zip values map { case (key, value) => client.set(key, value) }
    val writeResults = Future.sequence(writes).futureValue

    val reads = keys map { key => client.get(key) }
    val readResults = Future.sequence(reads).futureValue

    readResults zip values foreach { case (result, expectedValue) =>
      result should equal (Some(expectedValue))
    }
    readResults should equal (List(Some("value1"), Some("value2"), Some("value3")))
  }

  it("should give none for unknown keys") {
    client.get("get_unknown").futureValue should equal (None)
  }
}
```

```scala
describe("non blocking apis using futures") {
  // ...

  it("should compose with sequential combinator") {
    val key = "client_key_seq"

    val res = for {
      p <- client.lpush(key, 0 to 100)
      if p > 0
      r <- client.lrange[Long](key, 0, -1)
    } yield (p, r)

    val (count, list) = res.futureValue
    count should equal (101)
    list.reverse should equal (0 to 100)
  }
}

describe("error handling using promise failure") {
  it("should give error trying to lpush on a key that has a non list value") {
    val key = "client_err"
    client.set(key, "value200").futureValue should be (true)

    val thrown = evaluating {
      client.lpush(key, 1200).futureValue
    } should produce [TestFailedException]

    thrown.getCause.getMessage should equal ("ERR Operation against a key holding the wrong kind of value")
  }
}
```

#### Full set of Redis commands

##### List Operations

```scala
describe("lpush") {
  it("should do an lpush and retrieve the values using lrange") {
    val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)
    val writeList = forpush map { case (key, value) => client.lpush(key, value) }
    val writeListResults = Future.sequence(writeList).futureValue

    writeListResults foreach {
      case someLong: Long if someLong > 0 =>
        someLong should (be > (0L) and be <= (10L))
      case _ => fail("lpush must return a positive number")
    }
    writeListResults should equal (1 to 10)

    // do an lrange to check if they were inserted correctly & in proper order
    val readList = client.lrange[String]("listk", 0, -1)
    readList.futureValue should equal ((1 to 10).reverse.map(_.toString))
  }
}
```

##### Other Data Structure support as per Redis

```scala
// Hash
describe("hmget") {
  it("should set and get maps") {
    val key = "hmget1"
    client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))

    client
      .hmget(key, "field1")
      .futureValue should equal (Map("field1" -> "val1"))

    client
      .hmget(key, "field1", "field2")
      .futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))

    client
      .hmget(key, "field1", "field2", "field3")
      .futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
  }
}

// Set
describe("spop") {
  it("should pop a random element") {
    val key = "spop1"
    client.sadd(key, "foo")
    client.sadd(key, "bar")

    client
      .spop(key)
      .futureValue should (equal (Some("foo")) or equal (Some("bar")))
  }

  it("should return nil if the key does not exist") {
    val key = "spop2"
    client.spop(key).futureValue should equal (None)
  }
}

// Sorted Set
describe("z(rev)rangeByScoreWithScore") {
  it ("should return the elements between min and max") {
    add

    client
      .zrangeByScoreWithScores("hackers", 1940, true, 1969, true, None)
      .futureValue should equal (List(
        ("alan kay", 1940.0), ("richard stallman", 1953.0),
        ("yukihiro matsumoto", 1965.0), ("linus torvalds", 1969.0)
      ))

    client
      .zrevrangeByScoreWithScores("hackers", 1940, true, 1969, true, None)
      .futureValue should equal (List(
        ("linus torvalds", 1969.0), ("yukihiro matsumoto", 1965.0),
        ("richard stallman", 1953.0),("alan kay", 1940.0)
      ))

    client
      .zrangeByScoreWithScores("hackers", 1940, true, 1969, true, Some(3, 1))
      .futureValue should equal (List(("linus torvalds", 1969.0)))

    client
      .zrevrangeByScoreWithScores("hackers", 1940, true, 1969, true, Some(3, 1))
      .futureValue should equal (List(("alan kay", 1940.0)))
  }
}
 ```

#### Scripting support

```scala
import com.redis.serialization.DefaultFormats._

describe("eval") {
  it("should eval lua code and get a string reply") {
    client
      .eval("return 'val1';")
      .futureValue should equal (List("val1"))
  }

  it("should eval lua code and get a string array reply") {
    client
      .eval("return { 'val1','val2' };")
      .futureValue should equal (List("val1", "val2"))
  }

  it("should eval lua code and get a string array reply from its arguments") {
    client
      .eval("return { ARGV[1],ARGV[2] };", args = List("a", "b"))
      .futureValue should equal (List("a", "b"))
  }

  it("should eval lua code and get a string array reply from its arguments & keys") {
    client
      .eval("return { KEYS[1],KEYS[2],ARGV[1],ARGV[2] };", List("a", "b"), List("a", "b"))
      .futureValue should equal (List("a", "b", "a", "b"))
  }

  it("should eval lua code and get a string reply when passing keys") {
    client.set("a", "b")

    client
      .eval("return redis.call('get', KEYS[1]);", List("a"))
      .futureValue should equal (List("b"))
  }

  it("should eval lua code and get a string array reply when passing keys") {
    client.lpush("z", "a")
    client.lpush("z", "b")

    client
      .eval("return redis.call('lrange', KEYS[1], 0, 1);", keys = List("z"))
      .futureValue should equal (List("b", "a"))
  }

  it("should evalsha lua code hash and execute script when passing keys") {
    val setname = "records";

    val luaCode = """
          local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
          return res
          """
    val shahash = client.script.load(luaCode).futureValue

    client.zadd(setname, 10, "mmd")
    client.zadd(setname, 22, "mmc")
    client.zadd(setname, 12.5, "mma")
    client.zadd(setname, 14, "mem")

    val rs = client.evalsha(shahash.get, List("records")).futureValue
    rs should equal (List("mmd", "10", "mma", "12.5", "mem", "14", "mmc", "22"))
  }

  it("should check if script exists when passing its sha hash code") {
    val luaCode = """
          local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
          return res
          """
    val shahash = client.script.load(luaCode).futureValue

    val rs = client.script.exists(shahash.get).futureValue
    rs should equal (List(1))
  }

  it("should remove script cache") {
    val luaCode = """
          local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
          return res
          """
    val shahash = client.script.load(luaCode).futureValue

    client.script.flush.futureValue should be (true)

    client.script.exists(shahash.get).futureValue should equal (List(0))
  }
}
```

#### Typeclass based transparent serialization

###### Using built in Format typeclass for read and write

```scala
import DefaultFormats._

client
  .hmset("hash", Map("field1" -> "1", "field2" -> 2))
  .futureValue should be (true)

client
  .hmget[String]("hash", "field1", "field2")
  .futureValue should equal (Map("field1" -> "1", "field2" -> "2"))

client
  .hmget[Int]("hash", "field1", "field2")
  .futureValue should equal (Map("field1" -> 1, "field2" -> 2))

client
  .hmget[Int]("hash", "field1", "field2", "field3")
  .futureValue should equal (Map("field1" -> 1, "field2" -> 2))
```

###### Should use a provided implicit Format typeclass

```scala
import DefaultFormats._

client.hmset("hash", Map("field1" -> "1", "field2" -> 2))

implicit val intFormat = Format[Int](java.lang.Integer.parseInt, _.toString)

client
  .hmget[Int]("hash", "field1", "field2")
  .futureValue should equal (Map("field1" -> 1, "field2" -> 2))

client
  .hmget[String]("hash", "field1", "field2")
  .futureValue should be(Map("field1" -> "1", "field2" -> "2"))
```

###### Easy to have custom Format for case classes

```scala
case class Person(id: Int, name: String)

val debasish = Person(1, "Debasish Gosh")
val jisoo = Person(2, "Jisoo Park")

implicit val customPersonFormat =
  new Format[Person] {
    def read(str: String) = {
      val head :: rest = str.split('|').toList
      val id = head.toInt
      val name = rest.mkString("|")

      Person(id, name)
    }

    def write(person: Person) = {
      import person._
      s"$id|$name"
    }
  }

client.set("debasish", debasish)

client.get[Person]("debasish").futureValue should equal (Some(debasish))
```

###### Integration support with Json Serializing libraries

```scala
import spray.json.DefaultJsonProtocol._
import com.redis.serialization.SprayJsonSupport._

implicit val personFormat = jsonFormat2(Person)

client.set("debasish", debasish)
client.set("people", List(debasish, jisoo))

client.get[Person]("debasish").futureValue should equal (Some(debasish))
client.get[List[Person]]("people").futureValue should equal (Some(List(debasish, jisoo)))
```

For more examples on serialization, have a look at the test cases.

_Third-party libraries are not installed by default. You need to provide them by yourself._

#### Transaction Support

Implicit support of `MULTI`, `EXEC`, `DISCARD`, `WATCH` and `UNWATCH` provided through custom API. Here are some examples:

```scala
describe("transactions with API") {
  it("should use API") {
    val result = client.withTransaction {c =>
      c.set("anshin-1", "debasish") 
      c.exists("anshin-1")
      c.get("anshin-1")
      c.set("anshin-2", "debasish")
      c.lpush("anshin-3", "debasish") 
      c.lpush("anshin-3", "maulindu") 
      c.lrange("anshin-3", 0, -1)
    }
    result.futureValue should equal (List(true, true, Some("debasish"), true, 1, 2, List("maulindu", "debasish")))
    client.get("anshin-1").futureValue should equal(Some("debasish"))
  }

  it("should execute partial set and report failure on exec") {
    val result = client.withTransaction {c =>
      c.set("anshin-1", "debasish") 
      c.exists("anshin-1")
      c.get("anshin-1")
      c.set("anshin-2", "debasish")
      c.lpush("anshin-2", "debasish") 
      c.lpush("anshin-3", "maulindu") 
      c.lrange("anshin-3", 0, -1)
    }
    val r = result.futureValue.asInstanceOf[List[_]].toVector
    r(0) should equal(true)
    r(1) should equal(true)
    r(2) should equal(Some("debasish"))
    r(3) should equal(true)
    r(4).asInstanceOf[akka.actor.Status.Failure].cause.isInstanceOf[Throwable] should equal(true)
    r(5) should equal(1)
    r(6) should equal(List("maulindu"))
    client.get("anshin-1").futureValue should equal(Some("debasish"))
  }

  it("should fail if key is changed outside transaction") {
    client.watch("key1").futureValue should equal(true)
    client.set("key1", 1).futureValue should equal(true)
    val v = client.get[Long]("key1").futureValue.get + 20L
    val result = client.withTransaction {c =>
      c.set("key1", 100)
      c.get[Long]("key1")
    }
    result onComplete {
      case util.Success(_) => 
      case util.Failure(th) => th should equal(EmptyTxnResultException)
    }
  }
}
```


### License

This software is licensed under the Apache 2 license, quoted below.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
