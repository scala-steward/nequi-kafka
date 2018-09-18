package nequi.kafka.streams.statsd

import github.gphat.censorinus._

import net.manub.embeddedkafka._
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.streams._

import org.apache.kafka.streams.scala._

import utest._
import utest.framework.TestPath

object SetSpec extends TestSuite with EmbeddedKafkaStreamsAllInOne {
  import ImplicitConversions._
  import Serdes.String
  import imports._

  def dynConfig() = EmbeddedKafkaConfig(kafkaPort = getEmptyPort, zooKeeperPort = getEmptyPort)

  val tests = Tests {
    'set - set
  }

  def newClient() = new TestClient

  def set(implicit path: TestPath) = {
    implicit val c = dynConfig()

    val inTopic  = pathToTopic("-in")
    val outTopic = pathToTopic("-out")
    val client   = newClient()

    val builder = new StreamsBuilder
    val in      = builder.stream[String, String](inTopic)

    val out = in.setStat(client, pathToTopic(""), (k, v) => k)

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
      case SetMetric(name, value, _) if name == pathToTopic("") && value == "hello" =>
    }
  }
}
