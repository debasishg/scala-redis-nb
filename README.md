## redisreact

A non blocking Redis client based on Akka I/O

### Key features of the library

- Non blocking
- Full set of Redis commands
- Scripting support
- Transparent typeclass based serialization
- Out of the box integration support with Json serialization libraries
- Composable with Futures

### Project Dependencies

```scala
libraryDependencies ++= Seq(
  "net.debasishg.redisreact" %% "redisreact" % "0.1"
)
```

### Coming up

- Transcations
- Publish-Subscribe
- Clustering

### Sample usage

```scala
// Akka setup
implicit val system = ActorSystem("redis-client")
implicit val executionContext = system.dispatcher
implicit val timeout = AkkaTimeout(5 seconds)

// Redis client setup
val client = RedisClient("localhost", 6379)
```

#### Non Blocking Compositional Gets and Sets

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

#### List Operations

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

#### Other Data Structure support as per Redis

```scala
// Hash
describe("hmget") {
 it("should set and get maps") {
   val key = "hmget1"
   client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))
   client.hmget(key, "field1").futureValue should equal (Map("field1" -> "val1"))
   client.hmget(key, "field1", "field2").futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
   client.hmget(key, "field1", "field2", "field3").futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
 }
}

// Set
describe("spop") {
  it("should pop a random element") {
    val key = "spop1"
    client.sadd(key, "foo").futureValue should equal (1)
    client.sadd(key, "bar").futureValue should equal (1)
    client.sadd(key, "baz").futureValue should equal (1)
    client.spop(key).futureValue should (equal (Some("foo")) or equal (Some("bar")) or equal (Some("baz")))
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

    client.zrangeByScoreWithScores("hackers", 1940, true, 1969, true, None).futureValue should equal (
      List(("alan kay", 1940.0), ("richard stallman", 1953.0), ("yukihiro matsumoto", 1965.0), ("linus torvalds", 1969.0)))

    client.zrevrangeByScoreWithScores("hackers", 1940, true, 1969, true, None).futureValue should equal (
      List(("linus torvalds", 1969.0), ("yukihiro matsumoto", 1965.0), ("richard stallman", 1953.0),("alan kay", 1940.0)))

    client.zrangeByScoreWithScores("hackers", 1940, true, 1969, true, Some(3, 1)).futureValue should equal (
      List(("linus torvalds", 1969.0)))

    client.zrevrangeByScoreWithScores("hackers", 1940, true, 1969, true, Some(3, 1)).futureValue should equal (
      List(("alan kay", 1940.0)))
  }
}
 ```

#### With Operations that compose

```scala
describe("non blocking apis using futures") {
  it("get and set should be non blocking") {
    @volatile var callbackExecuted = false

    val ks = (1 to 10).map(i => s"client_key_$i")
    val kvs = ks.zip(1 to 10)

    val sets: Seq[Future[Boolean]] = kvs map {
      case (k, v) => client.set(k, v)
    }

    val setResult = Future.sequence(sets) map { r: Seq[Boolean] =>
      callbackExecuted = true
      r
    }

    callbackExecuted should be (false)
    setResult.futureValue should contain only (true)
    callbackExecuted should be (true)

    callbackExecuted = false
    val gets: Seq[Future[Option[Long]]] = ks.map { k => client.get[Long](k) }
    val getResult = Future.sequence(gets).map { rs =>
      callbackExecuted = true
      rs.flatten.sum
    }

    callbackExecuted should be (false)
    getResult.futureValue should equal (55)
    callbackExecuted should be (true)
  }

  it("should compose with sequential combinator") {
    val key = "client_key_seq"
    val values = (1 to 100).toList
    val pushResult = client.lpush(key, 0, values:_*)
    val getResult = client.lrange[Long](key, 0, -1)

    val res = for {
      p <- pushResult.mapTo[Long]
      if p > 0
      r <- getResult.mapTo[List[Long]]
    } yield (p, r)

    val (count, list) = res.futureValue
    count should equal (101)
    list.reverse should equal (0 to 100)
  }
}
```

```scala
describe("error handling using promise failure") {
  it("should give error trying to lpush on a key that has a non list value") {
    val key = "client_err"
    val v = client.set(key, "value200")
    v.futureValue should be (true)

    val x = client.lpush(key, 1200)
    val thrown = evaluating { x.futureValue } should produce [TestFailedException]
    thrown.getCause.getMessage should equal ("ERR Operation against a key holding the wrong kind of value")
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

client.hmset("hash", Map("field1" -> "1", "field2" -> 2)).futureValue should be (true)
client.hmget[String]("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "2"))
client.hmget[Int]("hash", "field1", "field2").futureValue should be(Map("field1" -> 1, "field2" -> 2))
client.hmget[Int]("hash", "field1", "field2", "field3").futureValue should be(Map("field1" -> 1, "field2" -> 2))
```

###### Should use a provided implicit Format typeclass

```scala
import DefaultFormats._

client.hmset("hash", Map("field1" -> "1", "field2" -> 2)).futureValue should be (true)
client.hmget("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "2"))

implicit val intFormat = Format[Int](java.lang.Integer.parseInt, _.toString)

client.hmget[Int]("hash", "field1", "field2").futureValue should be(Map("field1" -> 1, "field2" -> 2))
client.hmget[String]("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "2"))
client.hmget[Int]("hash", "field1", "field2", "field3").futureValue should be(Map("field1" -> 1, "field2" -> 2))
```

###### Easy to have custom Format for case classes

```scala
case class Person(id: Int, name: String)

val debasish = Person(1, "Debasish Gosh")
val jisoo = Person(2, "Jisoo Park")
val people = List(debasish, jisoo)

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

val write = implicitly[Write[Person]].write _
val read = implicitly[Read[Person]].read _

read(write(debasish)) should equal (debasish)
```

###### Integration support with Json Serializing libraries

```scala
import spray.json.DefaultJsonProtocol._
import SprayJsonSupport._

implicit val personFormat = jsonFormat2(Person)

val write = implicitly[Write[Person]].write _
val read = implicitly[Read[Person]].read _

val writeL = implicitly[Write[List[Person]]].write _
val readL = implicitly[Read[List[Person]]].read _

read(write(debasish)) should equal (debasish)
readL(writeL(people)) should equal (people)
```

For more examples on serialization, have a look at the test cases.

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
