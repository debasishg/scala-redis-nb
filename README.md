## Redis Scala client (Non blocking based on Akka IO)

### Key features of the library

- Native Scala types Set and List responses.
- Transparent serialization
- Non blocking
- Composable with Futures

### Sample usage

```scala
// Akka setup
implicit val system = ActorSystem("redis-client")
implicit val executionContext = system.dispatcher
implicit val timeout = AkkaTimeout(5 seconds)

// Redis client setup
val client = RedisClient("localhost", 6379)
```

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

```scala
describe("hmget") {
 it("should set and get maps") {
   val key = "hmget1"
   client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))
   client.hmget(key, "field1").futureValue should equal (Map("field1" -> "val1"))
   client.hmget(key, "field1", "field2").futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
   client.hmget(key, "field1", "field2", "field3").futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
 }
}
 ```

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
