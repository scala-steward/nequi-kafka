package nequi.kafka.streams.statsd

import github.gphat.censorinus.{ DogStatsDClient => DClient, StatsDClient => Client }

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{ Transformer, TransformerSupplier }
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state._

/** DataDog (StatsD + tags) **/
object DataDog {

  /** Count the number of messages coming through this stream **/
  def counted[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String]): KStream[K, V] =
    countedByMessage(stream, client, name, tags)((k, v) => Seq.empty)

  /** Count the number of messages coming through this stream, and add tags dependent on message**/
  def countedByMessage[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String])(
    f: (K, V) => Seq[String]
  ): KStream[K, V] =
    stream.peek((k, v) => client.increment(name, tags = tags ++ f(k, v)))

  /** Set a metric to a particular value **/
  def set[K, V](stream: KStream[K, V], client: DClient, name: String, tags: Seq[String])(
    f: (K, V) => String
  ): KStream[K, V] = stream.peek((k, v) => client.set(name, f(k, v), tags = tags))

  /** Report the processing time for stream operations in `f` **/
  def timed[K1, V1, K2, V2](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: DClient,
    name: String,
    tags: Seq[String]
  )(f: KStream[K1, V1] => KStream[K2, V2]): KStream[K2, V2] =
    StatsD.timed0((s, l) => client.timer(s, l, tags = tags))(stream)(builder, name, name)(f)

  def timed[K1, V1, K2, V2](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: DClient,
    name: String,
    storeName: String,
    tags: Seq[String]
  )(f: KStream[K1, V1] => KStream[K2, V2]): KStream[K2, V2] =
    StatsD.timed0((s, l) => client.timer(s, l, tags = tags))(stream)(builder, name, storeName)(f)
}

/** Plain StatsD **/
object StatsD {

  /** Count the number of messages coming through this stream **/
  def counted[K, V](stream: KStream[K, V], client: Client, name: String): KStream[K, V] = stream.peek { (_, _) =>
    client.increment(name)
  }

  /** Set a metric to a particular value **/
  def set[K, V](stream: KStream[K, V], client: Client, name: String)(f: (K, V) => String): KStream[K, V] = stream.peek {
    (k, v) => client.set(name, f(k, v))
  }

  /** Report the processing time for stream operations in `f` **/
  def timed[K1, V1, K2, V2](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: Client,
    name: String
  )(f: KStream[K1, V1] => KStream[K2, V2]): KStream[K2, V2] =
    timed0((s, l) => client.timer(s, l))(stream)(builder, name, name)(f)

  def timed[K1, V1, K2, V2](
    stream: KStream[K1, V1],
    builder: StreamsBuilder,
    client: Client,
    name: String,
    storeName: String
  )(f: KStream[K1, V1] => KStream[K2, V2]): KStream[K2, V2] =
    timed0((s, l) => client.timer(s, l))(stream)(builder, name, storeName)(f)

  private[statsd] def timed0[K1, V1, K2, V2](report: (String, Long) => Unit)(
    stream: KStream[K1, V1]
  )(builder: StreamsBuilder, name: String, storeName: String)(
    f: KStream[K1, V1] => KStream[K2, V2]
  ): KStream[K2, V2] = {
    val stateStoreName = s"statsd-${name}-${storeName}"

    builder.addStateStore(
      Stores
        .keyValueStoreBuilder[String, Long](Stores.inMemoryKeyValueStore(stateStoreName), Serdes.String, Serdes.Long)
    )

    val startTime: KStream[K1, V1] = stream.transform(
      new TransformerSupplier[K1, V1, KeyValue[K1, V1]] {
        override def get(): Transformer[K1, V1, KeyValue[K1, V1]] =
          new Transformer[K1, V1, KeyValue[K1, V1]] {
            var store: KeyValueStore[String, Long] = _
            var taskId: String                     = _

            def init(ctx: ProcessorContext): Unit = {
              store = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Long]]
              taskId = ctx.taskId.toString
            }
            def close: Unit = store.close

            def transform(k: K1, v: V1): KeyValue[K1, V1] = {
              store.put(taskId, System.currentTimeMillis)

              new KeyValue(k, v)
            }
          }
      },
      stateStoreName
    )

    f(startTime).transform(
      new TransformerSupplier[K2, V2, KeyValue[K2, V2]] {
        override def get(): Transformer[K2, V2, KeyValue[K2, V2]] =
          new Transformer[K2, V2, KeyValue[K2, V2]] {
            var store: KeyValueStore[String, Long] = _
            var taskId: String                     = _

            def init(ctx: ProcessorContext): Unit = {
              store = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Long]]
              taskId = ctx.taskId.toString
            }
            def close: Unit = store.close

            def transform(k: K2, v: V2): KeyValue[K2, V2] = {
              Option(store.get(taskId)).foreach { startTime =>
                store.delete(taskId)

                val now = System.currentTimeMillis
                report(name, now - startTime)
              }

              new KeyValue(k, v)
            }
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
    def timed[K2, V2](builder: StreamsBuilder, client: Client, name: String, storeName: String)(
      f: KStream[K, V] => KStream[K2, V2]
    ) =
      StatsD.timed(s, builder, client, name, storeName)(f)
    def timed[K2, V2](builder: StreamsBuilder, client: DClient, name: String, tags: Seq[String])(
      f: KStream[K, V] => KStream[K2, V2]
    ) = DataDog.timed(s, builder, client, name, tags)(f)
    def timed[K2, V2](builder: StreamsBuilder, client: DClient, name: String, storeName: String, tags: Seq[String])(
      f: KStream[K, V] => KStream[K2, V2]
    ) = DataDog.timed(s, builder, client, name, storeName, tags)(f)
  }
}
