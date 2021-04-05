package lerna.akka.entityreplication.util

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ Future, Promise }

object AtLeastOnceComplete {
  def askTo(
      destination: ActorRef,
      message: Any,
      retryInterval: FiniteDuration,
      fizz: String = "fizz",
  )(implicit system: ActorSystem, timeout: Timeout): Future[Any] = {

    val logging = Logging(system, this.getClass)

    import system.dispatcher
    val promise = Promise[Any]()

    def send(): Unit = {
      val future = destination ? message
      promise.completeWith(future)
    }

    send()

    val cancellable = system.scheduler.scheduleAtFixedRate(
      initialDelay = retryInterval,
      interval = retryInterval,
    ) { () =>
      logging.debug(
        "Destination {} did not reply to a message in {}. Retrying to send the message [{}].",
        destination,
        retryInterval,
        message,
      )
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
