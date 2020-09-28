package lerna.akka.entityreplication.util

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ Future, Promise }

object AtLeastOnceComplete {
  def askTo(
      destination: ActorRef,
      message: Any,
  )(implicit system: ActorSystem, timeout: Timeout): Future[Any] = {

    import system.dispatcher
    val promise = Promise[Any]()

    def send(): Unit = {
      val future = destination ? message
      promise.tryCompleteWith(future)
    }

    val config = system.settings.config.getConfig("lerna.akka.entityreplication.util.at-least-once-complete")

    import JavaDurationConverters._
    val retryInterval: FiniteDuration = config.getDuration("retry-interval").asScala

    send()

    val cancellable = system.scheduler.scheduleAtFixedRate(
      initialDelay = retryInterval,
      interval = retryInterval,
    ) { () =>
      send()
    }

    promise.future.onComplete { _ =>
      cancellable.cancel()
    }
    system.scheduler.scheduleOnce(timeout.duration) {
      cancellable.cancel()
    }

    promise.future
  }
}
