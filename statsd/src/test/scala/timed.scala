package nequi.kafka.streams.statsd

import github.gphat.censorinus._

import net.manub.embeddedkafka._
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.streams._

import org.apache.kafka.streams.scala._

import utest._
import utest.framework.TestPath

object TimedSpec extends TestSuite with EmbeddedKafkaStreamsAllInOne {
  import ImplicitConversions._
  import Serdes.String
  import imports._

  def dynConfig() = EmbeddedKafkaConfig(kafkaPort = getEmptyPort, zooKeeperPort = getEmptyPort)

  val tests = Tests {
    'timeOne - timeOne
    'timeTwo - timeTwo
  }

  def newClient() = new TestClient

  def timeOne(implicit path: TestPath) = {
    implicit val c = dynConfig()
    val inTopic    = pathToTopic("-in")
    val outTopic   = pathToTopic("-out")
    val client     = newClient()

    val builder = new StreamsBuilder
    val in      = builder.stream[String, String](inTopic)

    val out = in.timed[String, String](builder, client, "timed") { stream =>
      stream.mapValues { v =>
        Thread.sleep(1000L)
        v
      }
    }

    out.to(outTopic)

    runStreams(Seq(inTopic, outTopic), builder.build()) {
      publishToKafka(inTopic, "hello", "world")

      withConsumer[String, String, Unit] { consumer =>
        val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
        consumedMessages.head ==> ("hello" -> "world")
      }
    }

    client.q.size ==> 1
    val m = client.q.poll

    assertMatch(m) {
      case TimerMetric(name, value, _, _) if name == "timed" && value >= 1000L =>
    }
  }

  def timeTwo(implicit path: TestPath) = {
    implicit val c = dynConfig()
    val inTopic    = pathToTopic("-in")
    val outTopic   = pathToTopic("-out")
    val client     = newClient()

    val builder = new StreamsBuilder
    val in      = builder.stream[String, String](inTopic)

    val out = in.timed[String, String](builder, client, "timed") { stream =>
      stream.mapValues(_ match {
        case v: String if v == "world" =>
          Thread.sleep(1000L)
          v
        case v: String =>
          Thread.sleep(5000L)
          v
      })
    }

    out.to(outTopic)

    runStreams(Seq(inTopic, outTopic), builder.build()) {
      publishToKafka(inTopic, "hello", "world")
      publishToKafka(inTopic, "hello", "foo")

      Thread.sleep(5000L)

      withConsumer[String, String, Unit] { consumer =>
        val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
        consumedMessages.head ==> ("hello"         -> "world")
        consumedMessages.drop(1).head ==> ("hello" -> "foo")
      }
    }

    client.q.size ==> 2
    val m = client.q.poll

    assertMatch(m) {
      case TimerMetric(name, value, _, _) if name == "timed" && value >= 1000L && value < 5000L =>
    }

    val m2 = client.q.poll

    assertMatch(m2) {
      case TimerMetric(name, value, _, _) if name == "timed" && value >= 5000L =>
    }
  }
}
