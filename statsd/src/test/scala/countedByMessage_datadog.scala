package nequi.kafka.streams.statsd

import scala.util.Random

import github.gphat.censorinus._

import net.manub.embeddedkafka._
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.streams._

import org.apache.kafka.streams.scala._

import utest._
import utest.framework.TestPath

object CountedByMessageDataDogSpec extends TestSuite {
  import EmbeddedKafkaStreams._
  import ImplicitConversions._
  import Serdes.String
  import imports._

  def dynConfig() = EmbeddedKafkaConfig(kafkaPort = getEmptyPort, zooKeeperPort = getEmptyPort)

  val tests = Tests {
    'countOne - countOne
    'countMany - countMany
  }

  def newClient() = new TestDataDogClient

  def countOne(implicit path: TestPath) = {
    implicit val c = dynConfig()

    val inTopic  = pathToTopic("-in")
    val outTopic = pathToTopic("-out")
    val client   = newClient()
    val tags     = Seq(pathToTopic("1"), pathToTopic("2"))

    val builder = new StreamsBuilder
    val in      = builder.stream[String, String](inTopic)

    val f   = (k: String, _: String) => Seq(k)
    val out = in.countedByMessage(client, pathToTopic(""), tags)(f)

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
      case CounterMetric(name, value, _, t)
          if name == pathToTopic("") && value == 1L && t == tags ++ f("hello", "world") =>
    }
  }

  def countMany(implicit path: TestPath) = {
    implicit val c = dynConfig()

    val inTopic  = pathToTopic("-in")
    val outTopic = pathToTopic("-out")
    val client   = newClient()
    val tags     = Seq(pathToTopic("1"), pathToTopic("2"))

    val builder = new StreamsBuilder
    val in      = builder.stream[String, String](inTopic)

    val f   = (k: String, _: String) => Seq(k)
    val out = in.countedByMessage(client, pathToTopic(""), tags)(f)

    out.to(outTopic)
    val numberOfMessages = Random.nextInt(100)

    runStreams(Seq(inTopic, outTopic), builder.build()) {
      (0 until numberOfMessages).foreach(a => publishToKafka(inTopic, s"${a}", "world"))

      withConsumer[String, String, Unit] { consumer =>
        val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
        consumedMessages.head ==> ("0" -> "world")
      }
    }

    client.q.size ==> numberOfMessages
    val m = client.q.poll

    assertMatch(m) {
      case CounterMetric(name, value, _, t) if name == pathToTopic("") && value == 1L && t == tags ++ f("0", "world") =>
    }

    val m2 = client.q.poll

    assertMatch(m2) {
      case CounterMetric(name, value, _, t) if name == pathToTopic("") && value == 1L && t == tags ++ f("1", "world") =>
    }
  }
}
