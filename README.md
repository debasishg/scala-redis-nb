## Redis Scala client (Non blocking based on Akka IO)

### Key features of the library

- Native Scala types Set and List responses.
- Transparent serialization
- Non blocking
- Composable with Futures

### Sample usage

```scala
describe("set") {
  it("should set values to keys") {
    val numKeys = 3
    val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
    val writes = keys zip values map { case (key, value) => set(key, value) apply client }

    writes foreach { _ onSuccess {
      case true => 
      case _ => fail("set should pass")
    }}
  }
}
```

```scala
describe("get") {
  it("should get results for keys set earlier") {
    val numKeys = 3
    val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
    val reads = keys map { key => get(key) apply client }

    reads zip values foreach { case (result, expectedValue) =>
      result.onSuccess {
        case resultString => resultString should equal(Some(expectedValue))
      }
      result.onFailure {
        case t => println("Got some exception " + t)
      }
    }
    reads.map(e => Await.result(e, 3 seconds)) should equal(List(Some("value1"), Some("value2"), Some("value3")))
  }
  it("should give none for unknown keys") {
    val reads = get("key10") apply client
    Await.result(reads, 3 seconds) should equal(None)
  }
}
```

```scala
describe("lpush") {
  it("should do an lpush and retrieve the values using lrange") {
    val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)

    val writeListResults = forpush map { case (key, value) =>
      (key, value, (lpush(key, value) apply client))
    }

    writeListResults foreach { case (key, value, result) =>
      result onSuccess {
        case someLong: Long if someLong > 0 => {
          someLong should (be > (0L) and be <= (10L))
        }
        case _ => fail("lpush must return a positive number")
      }
    }
    writeListResults.map(e => Await.result(e._3, 3 seconds)) should equal((1 to 10).toList)

    // do an lrange to check if they were inserted correctly & in proper order
    val readListResult = lrange[String]("listk", 0, -1) apply client
    readListResult.onSuccess {
      case result => result should equal ((1 to 10).reverse.toList)
    }
  }
}
```

```scala
describe("non blocking apis using futures") {
  it("get and set should be non blocking") {
    val kvs = (1 to 10).map(i => s"key_$i").zip(1 to 10)
    val setResults: Seq[Future[Boolean]] = kvs map {case (k, v) =>
      set(k, v) apply client
    }
    val sr = Future.sequence(setResults)

    Await.result(sr.map(x => x), 2 seconds).forall(_ == true) should equal(true)

    val ks = (1 to 10).map(i => s"key_$i")
    val getResults = ks.map {k =>
      get[Long](k) apply client
    }

    val gr = Future.sequence(getResults)
    val result = gr.map(_.flatten.sum)

    Await.result(result, 2 seconds) should equal(55)
  }

  it("should compose with sequential combinator") {
    val values = (1 to 100).toList
    val pushResult = lpush("key", 0, values:_*) apply client
    val getResult = lrange[Long]("key", 0, -1) apply client
    
    val res = for {
      p <- pushResult.mapTo[Long]
      if p > 0
      r <- getResult.mapTo[List[Long]]
    } yield (p, r)

    val (count, list) = Await.result(res, 2 seconds)
    count should equal(101)
    list.reverse should equal(0 to 100)
  }
}
```

```scala
describe("error handling using promise failure") {
  it("should give error trying to lpush on a key that has a non list value") {
    val v = set("key200", "value200") apply client 
    Await.result(v, 3 seconds) should equal(true)

    val x = lpush("key200", 1200) apply client

    x onSuccess {
      case someLong: Long => fail("lpush should fail")
      case _ => fail("lpush must return error")
    }
    x onFailure {
      case t => {
        val thrown = evaluating { Await.result(x, 3 seconds) } should produce [Exception]
        thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
      }
    }

    val thrown = evaluating {
      Await.result(x, 3 seconds)
    } should produce [Exception]
    thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
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
