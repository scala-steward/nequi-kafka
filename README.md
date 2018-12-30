# nequi-kafka
![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.nequissimus/kafka-streams-statsd_2.12/badge.svg)

Kafka (Streams) extensions/helpers

## StatsD + DataDog

Small extensions that help with stats collection towards StatsD and DataDog

```scala
libraryDependencies += "com.nequissimus" %% "kafka-streams-statsd" % "<VERSION>"
```

```scala
import nequi.kafka.streams.statsd.imports._

// This will bring in extensions to KStream[K, V]
```
