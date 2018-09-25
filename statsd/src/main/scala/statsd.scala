package nequi.kafka.streams.statsd

import github.gphat.censorinus.{ DogStatsDClient => DClient, StatsDClient => Client }

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state._

/** DataDog (StatsD + tags) **/
object DataDog {

  /** Count the number of messages coming through this stream **/
  def counted[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String]): KStream[K, V] =
    countedByMessage(stream, client, name, tags)((k, v) => Seq.empty)

  // FIXME: This should be stream.peek but need to wait for a release with
  // https://github.com/apache/kafka/pull/5566
  /** Count the number of messages coming through this stream, and add tags dependent on message**/
  def countedByMessage[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String])(
    f: (K, V) => Seq[String]
  ): KStream[K, V] =
    stream.map { (k, v) =>
      client.increment(name, tags = tags ++ f(k, v))
      (k, v)
    }

  // FIXME: This should be stream.peek but need to wait for a release with
  // https://github.com/apache/kafka/pull/5566
  /** Set a metric to a particular value **/
  def set[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String])(
    f: (K, V) => String
  ): KStream[K, V] = stream.map { (k, v) =>
    client.set(name, f(k, v), tags = tags)
    (k, v)
  }

  /** Report the processing time for stream operations in `f` **/
  def timed[K1, V1, K2, V2](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: DClient,
    name: String,
    tags: Seq[String]
  )(f: KStream[K1, V1] => KStream[K2, V2]): KStream[K2, V2] =
    StatsD.timed0((s, l) => client.timer(s, l, tags = tags))(stream)(builder, name)(f)

}

/** Plain StatsD **/
object StatsD {
  // FIXME: This should be stream.peek but need to wait for a release with
  // https://github.com/apache/kafka/pull/5566
  /** Count the number of messages coming through this stream **/
  def counted[K, V](stream: KStream[K, V], client: Client, name: String): KStream[K, V] = stream.mapValues { v =>
    client.increment(name)
    v
  }

  // FIXME: This should be stream.peek but need to wait for a release with
  // https://github.com/apache/kafka/pull/5566
  /** Set a metric to a particular value **/
  def set[K, V](stream: KStream[K, V], client: Client, name: String)(f: (K, V) => String): KStream[K, V] = stream.map {
    (k, v) =>
      client.set(name, f(k, v))
      (k, v)
  }

  /** Report the processing time for stream operations in `f` **/
  def timed[K1, V1, K2, V2](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: Client,
    name: String
  )(f: KStream[K1, V1] => KStream[K2, V2]): KStream[K2, V2] =
    timed0((s, l) => client.timer(s, l))(stream)(builder, name)(f)

  private[statsd] def timed0[K1, V1, K2, V2](report: (String, Long) => Unit)(
    stream: KStream[K1, V1]
  )(builder: StreamsBuilder, name: String)(
    f: KStream[K1, V1] => KStream[K2, V2]
  ): KStream[K2, V2] = {
    val stateStoreName = s"statsd-${name}"

    builder.addStateStore(
      Stores
        .keyValueStoreBuilder[String, Long](Stores.inMemoryKeyValueStore(stateStoreName), Serdes.String, Serdes.Long)
    )

    val startTime: KStream[K1, V1] = stream.transform(
      new Transformer[K1, V1, (K1, V1)] {
        var store: KeyValueStore[String, Long] = _
        var taskId: String                     = _

        def init(ctx: ProcessorContext): Unit = {
          store = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Long]]
          taskId = ctx.taskId.toString
        }
        def close: Unit = store.close

        def transform(k: K1, v: V1): (K1, V1) = {
          store.put(taskId, System.currentTimeMillis)

          (k, v)
        }
      },
      stateStoreName
    )

    f(startTime).transform(
      new Transformer[K2, V2, (K2, V2)] {
        var store: KeyValueStore[String, Long] = _
        var taskId: String                     = _

        def init(ctx: ProcessorContext): Unit = {
          store = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Long]]
          taskId = ctx.taskId.toString
        }
        def close: Unit = store.close

        def transform(k: K2, v: V2): (K2, V2) = {
          Option(store.get(taskId)).foreach { startTime =>
            store.delete(taskId)

            val now = System.currentTimeMillis
            report(name, now - startTime)
          }

          (k, v)
        }
      },
      stateStoreName
    )
  }
}

object imports {
  implicit class StreamsOps[K, V](s: KStream[K, V]) {
    def counted(c: Client, n: String): KStream[K, V]                     = StatsD.counted(s, c, n)
    def counted(c: DClient, n: String, tags: Seq[String]): KStream[K, V] = DataDog.counted(s, c, n, tags)
    def countedByMessage(c: DClient, n: String, tags: Seq[String])(f: (K, V) => Seq[String]): KStream[K, V] =
      DataDog.countedByMessage(s, c, n, tags)(f)
    def setStat(c: Client, n: String, f: (K, V) => String): KStream[K, V] = StatsD.set(s, c, n)(f)
    def setStat(c: DClient, n: String, tags: Seq[String], f: (K, V) => String): KStream[K, V] =
      DataDog.set(s, c, n, tags)(f)
    def timed[K2, V2](builder: StreamsBuilder, client: Client, name: String)(f: KStream[K, V] => KStream[K2, V2]) =
      StatsD.timed(s, builder, client, name)(f)
    def timed[K2, V2](builder: StreamsBuilder, client: DClient, name: String, tags: Seq[String])(
      f: KStream[K, V] => KStream[K2, V2]
    ) = DataDog.timed(s, builder, client, name, tags)(f)
  }
}
