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
  // FIXME: This should be stream.peek but need to wait for a release with
  // https://github.com/apache/kafka/pull/5566
  /** Count the number of messages coming through this stream **/
  def counted[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String]): KStream[K, V] =
    stream.mapValues { v =>
      client.increment(name, tags = tags)
      v
    }

  /** Report the processing time for stream operations in `f`
   * Note that in order to identify start and end times, we need functions `id1` and `id2`.
   * These should return the same ID for the corresponding records before and after applying `f`.
   */
  def timed[K1, V1, K2, V2, ID](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: DClient,
    name: String,
    id1: (K1, V1) => ID,
    id2: (K2, V2) => ID,
    tags: Seq[String]
  )(f: KStream[K1, V1] => KStream[K2, V2])(implicit IDS: Serde[ID]): KStream[K2, V2] =
    StatsD.timed0((s, l) => client.timer(s, l, tags = tags))(stream)(builder, name, id1, id2)(f)(IDS)

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

  /** Report the processing time for stream operations in `f`
   * Note that in order to identify start and end times, we need functions `id1` and `id2`.
   * These should return the same ID for the corresponding records before and after applying `f`.
   */
  def timed[K1, V1, K2, V2, ID](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: Client,
    name: String,
    id1: (K1, V1) => ID,
    id2: (K2, V2) => ID
  )(f: KStream[K1, V1] => KStream[K2, V2])(implicit IDS: Serde[ID]): KStream[K2, V2] =
    timed0((s, l) => client.timer(s, l))(stream)(builder, name, id1, id2)(f)(IDS)

  private[statsd] def timed0[K1, V1, K2, V2, ID](report: (String, Long) => Unit)(
    stream: KStream[K1, V1]
  )(builder: StreamsBuilder, name: String, id1: (K1, V1) => ID, id2: (K2, V2) => ID)(
    f: KStream[K1, V1] => KStream[K2, V2]
  )(implicit IDS: Serde[ID]): KStream[K2, V2] = {
    val stateStoreName = s"statsd-${name}"

    builder.addStateStore(
      Stores.keyValueStoreBuilder[ID, Long](Stores.inMemoryKeyValueStore(stateStoreName), IDS, Serdes.Long)
    )

    val startTime: KStream[K1, V1] = stream.transform(
      new Transformer[K1, V1, (K1, V1)] {
        var store: KeyValueStore[ID, Long] = _

        def init(ctx: ProcessorContext): Unit =
          store = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[ID, Long]]
        def close: Unit = store.close

        def transform(k: K1, v: V1): (K1, V1) = {
          store.put(id1(k, v), System.currentTimeMillis)

          (k, v)
        }
      },
      stateStoreName
    )

    f(startTime).transform(
      new Transformer[K2, V2, (K2, V2)] {
        var store: KeyValueStore[ID, Long] = _

        def init(ctx: ProcessorContext): Unit =
          store = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[ID, Long]]
        def close: Unit = store.close

        def transform(k: K2, v: V2): (K2, V2) = {
          val id = id2(k, v)
          Option(store.get(id)).foreach { startTime =>
            store.delete(id)

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
    def timed[K2, V2, ID](builder: StreamsBuilder, client: Client, name: String)(
      id1: (K, V) => ID,
      id2: (K2, V2) => ID
    )(f: KStream[K, V] => KStream[K2, V2])(implicit IDS: Serde[ID]) =
      StatsD.timed(s, builder, client, name, id1, id2)(f)
    def timed[K2, V2, ID](builder: StreamsBuilder, client: DClient, name: String)(
      id1: (K, V) => ID,
      id2: (K2, V2) => ID,
      tags: Seq[String]
    )(f: KStream[K, V] => KStream[K2, V2])(implicit IDS: Serde[ID]) =
      DataDog.timed(s, builder, client, name, id1, id2, tags)(f)
  }
}
