## Redis Scala client (Non blocking based on Akka IO)

### Key features of the library

- Native Scala types Set and List responses.
- Transparent serialization
- Non blocking
- Composable with Futures

### Sample usage

```scala
val kvs = (1 to 10).map(i => s"key_$i").zip(1 to 10)
val setResults = kvs map {case (k, v) =>
  set(k, v) apply client
}
val sr = Future.sequence(setResults)

Await.result(sr.map(_.flatten), 2 seconds).forall(_ == true) should equal(true)

val ks = (1 to 10).map(i => s"key_$i")
val getResults = ks.map {k =>
  get[Long](k) apply client
}

val gr = Future.sequence(getResults)
val result = gr.map(_.flatten.sum)

Await.result(result, 2 seconds) should equal(55)
```

```scala
val values = (1 to 100).toList
val pushResult = lpush("key", 0, values:_*) apply client
val getResult = lrange[Long]("key", 0, -1) apply client
      
val res = for {
  p <- pushResult.mapTo[Option[Long]]
  if p.get > 0
  r <- getResult.mapTo[Option[List[Long]]]
} yield (p, r)

val (count, list) = Await.result(res, 2 seconds)
count should equal(Some(101))
list.get.reverse should equal((0 to 100).map(a => Some(a)))
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
