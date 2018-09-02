package nequi.kafka.streams.statsd

import java.util.concurrent.LinkedBlockingQueue

import github.gphat.censorinus._

final class TestClient extends StatsDClient(asynchronous = false) {
  val q: LinkedBlockingQueue[Metric] = new LinkedBlockingQueue[Metric]

  override def enqueue(metric: Metric, sampleRate: Double = defaultSampleRate, bypassSampler: Boolean = false): Unit = {
    val _ = q.offer(metric)
  }
}

final class TestDataDogClient extends DogStatsDClient(asynchronous = false) {
  val q: LinkedBlockingQueue[Metric] = new LinkedBlockingQueue[Metric]

  override def enqueue(metric: Metric, sampleRate: Double = defaultSampleRate, bypassSampler: Boolean = false): Unit = {
    val _ = q.offer(metric)
  }
}
