package lerna.akka.entityreplication.rollback
import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import akka.stream.scaladsl.Sink

import java.time.Instant
import scala.concurrent.Future

/** @inheritdoc */
private final class LinearRollbackTimestampHintFinder(
    systemProvider: ClassicActorSystemProvider,
    queries: PersistenceQueries,
) extends RollbackTimestampHintFinder {

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  import system.dispatcher

  /** @inheritdoc */
  override def findTimestampHint(
      requirements: PersistentActorRollback.RollbackRequirements,
  ): Future[RollbackTimestampHintFinder.TimestampHint] = {
    // NOTE: In most cases, an event with `lowestSequenceNr` or `lowestSequenceNr+1` exists.
    // TODO Search a timestamp hint from snapshots since the persistent actor can delete the event with `lowestSequenceNr`.
    queries
      .currentEventsAfter(requirements.persistenceId, requirements.lowestSequenceNr)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(hintEvent) =>
          val hint = RollbackTimestampHintFinder.TimestampHint(
            requirements.persistenceId,
            hintEvent.sequenceNr,
            Instant.ofEpochMilli(hintEvent.timestamp),
          )
          Future.successful(hint)
        case None =>
          Future.failed(
            new RollbackTimestampHintNotFound(
              s"no events of persistenceId=[${requirements.persistenceId}] with a sequence number" +
              s" greater than or equal to lowestSequenceNr=[${requirements.lowestSequenceNr.value}]",
            ),
          )
      }
  }

}
