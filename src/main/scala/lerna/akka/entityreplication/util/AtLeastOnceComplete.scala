package lerna.akka.entityreplication.util

import akka.actor.{ ActorRef, ActorSystem }
import akka.actor.typed
import akka.actor.typed.RecipientRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.event.{ Logging, LoggingAdapter }
import akka.pattern.{ ask, StatusReply }
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ Future, Promise }

object AtLeastOnceComplete {

  /**
    * Asks the destination with retrying until timeout.
    * The message will be sent multiple times for each retryInterval.
    * It will stop when it receives the response from the destination.
    * Note that the destination may be receive the same message even if it responds due to a delay in message arrival.
    * The returned Future will be completed with an [[akka.pattern.AskTimeoutException]] after the given timeout has expired.
    */
  def askTo[Message, Reply](
      destination: RecipientRef[Message],
      message: typed.ActorRef[Reply] => Message,
      retryInterval: FiniteDuration,
  )(implicit system: typed.ActorSystem[_], timeout: Timeout): Future[Reply] =
    internalAskTo(
      { (retrying: Boolean, logging: LoggingAdapter) =>
        destination ask { replyTo: typed.ActorRef[Reply] =>
          val msg = message(replyTo)
          if (retrying) {
            logging.warning(
              "Destination {} did not reply to a message in {}. Retrying to send the message [{}].",
              destination,
              retryInterval,
              msg,
            )
          }
          msg
        }
      },
      retryInterval,
      system.toClassic,
    )

  /**
    * Asks the destination with retrying until timeout.
    * The message will be sent multiple times for each retryInterval.
    * It will stop when it receives the response from the destination.
    * Note that the destination may be receive the same message even if it responds due to a delay in message arrival.
    * The returned Future will be completed with an [[akka.pattern.AskTimeoutException]] after the given timeout has expired.
    * The Future will also failed if a [[akka.pattern.StatusReply.Error]] arrives.
    */
  def askWithStatusTo[Message, Reply](
      destination: RecipientRef[Message],
      message: typed.ActorRef[StatusReply[Reply]] => Message,
      retryInterval: FiniteDuration,
  )(implicit system: typed.ActorSystem[_], timeout: Timeout): Future[Reply] =
    internalAskTo(
      { (retrying: Boolean, logging: LoggingAdapter) =>
        destination askWithStatus { replyTo: typed.ActorRef[StatusReply[Reply]] =>
          val msg = message(replyTo)
          if (retrying) {
            logging.warning(
              "Destination {} did not reply to a message in {}. Retrying to send the message [{}].",
              destination,
              retryInterval,
              msg,
            )
          }
          msg
        }
      },
      retryInterval,
      system.toClassic,
    )

  def askTo(
      destination: ActorRef,
      message: Any,
      retryInterval: FiniteDuration,
  )(implicit system: ActorSystem, timeout: Timeout): Future[Any] =
    internalAskTo(
      { (retrying: Boolean, logging: LoggingAdapter) =>
        if (retrying) {
          logging.warning(
            "Destination {} did not reply to a message in {}. Retrying to send the message [{}].",
            destination,
            retryInterval,
            message,
          )
        }
        destination ? message
      },
      retryInterval,
      system,
    )

  private[this] trait AskStrategy[Reply] {
    def apply(retrying: Boolean, logging: LoggingAdapter): Future[Reply]
  }

  private[this] def internalAskTo[Reply](
      ask: AskStrategy[Reply],
      retryInterval: FiniteDuration,
      system: ActorSystem,
  )(implicit timeout: Timeout): Future[Reply] = {

    val logging = Logging(system, this.getClass)

    import system.dispatcher
    val promise = Promise[Reply]()

    promise.completeWith(ask(retrying = false, logging))

    val cancellable = system.scheduler.scheduleAtFixedRate(
      initialDelay = retryInterval,
      interval = retryInterval,
    ) { () =>
      promise.completeWith(ask(retrying = true, logging))
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
