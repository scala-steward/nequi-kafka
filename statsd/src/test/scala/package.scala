package nequi.kafka.streams

import java.net.ServerSocket

import scala.util.Try

import utest.framework.TestPath

package object statsd {
  def pathToTopic(suffix: String)(implicit path: TestPath) = suffix + path.value.mkString("-").toLowerCase

  def getEmptyPort: Int =
    Try {
      val socket = new ServerSocket(0)
      socket.close
      socket.getLocalPort
    }.getOrElse(getEmptyPort) // RIP stack
}
