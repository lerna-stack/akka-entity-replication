package lerna.akka.entityreplication.rollback

import akka.Done
import akka.actor.{ ActorSystem, ClassicActorSystemProvider }

import java.time.Instant
import scala.concurrent.Future

/** @inheritdoc */
private final class DefaultRollbackRequirementsVerifier(
    systemProvider: ClassicActorSystemProvider,
    rollback: PersistentActorRollback,
    timestampHintFinder: RollbackTimestampHintFinder,
) extends RollbackRequirementsVerifier {

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  import system.dispatcher

  /** @inheritdoc */
  override def verify(
      persistenceId: String,
      toSequenceNr: Option[SequenceNr],
      toTimestampHintOpt: Option[Instant],
  ): Future[Done] = {
    rollback.findRollbackRequirements(persistenceId).flatMap { requirements =>
      assert(requirements.persistenceId == persistenceId)
      toSequenceNr match {
        case Some(sequenceNr) =>
          if (sequenceNr >= requirements.lowestSequenceNr) {
            Future.successful(Done)
          } else {
            tryFindRollbackTimestampHint(requirements, toTimestampHintOpt)
              .flatMap { rollbackTimestampHintOpt =>
                Future.failed(
                  new RollbackRequirementsNotFulfilled(
                    s"Rollback to sequence number [${sequenceNr.value}] for the persistent actor [${persistenceId}] is impossible" +
                    s" since the sequence number should be greater than or equal to [${requirements.lowestSequenceNr.value}]." +
                    rollbackTimestampHintOpt.fold("")(" " + _),
                  ),
                )
              }
          }
        case None =>
          if (requirements.lowestSequenceNr == SequenceNr(1)) {
            Future.successful(Done)
          } else {
            tryFindRollbackTimestampHint(requirements, toTimestampHintOpt)
              .flatMap { rollbackTimestampHintOpt =>
                Future.failed(
                  new RollbackRequirementsNotFulfilled(
                    s"Deleting all data for the persistent actor [${persistenceId}] is impossible" +
                    " since already deleted events might contain required data for consistency with other persistent actors." +
                    rollbackTimestampHintOpt.fold("")(" " + _),
                  ),
                )
              }
          }
      }
    }
  }

  private def tryFindRollbackTimestampHint(
      requirements: PersistentActorRollback.RollbackRequirements,
      toTimestampHintOpt: Option[Instant],
  ): Future[Option[String]] = {
    toTimestampHintOpt match {
      case Some(toTimestamp) =>
        timestampHintFinder
          .findTimestampHint(requirements)
          .transform { hintOrError =>
            hintOrError
              .map { hint =>
                s"Hint: rollback timestamp [$toTimestamp] should be newer than timestamp [${hint.timestamp}] of sequence number [${hint.sequenceNr.value}], at least."
              }.recover { cause =>
                s"Hint: not available due to [${cause.getClass.getCanonicalName}]: ${cause.getMessage}]."
              }
              .map(Option.apply)
          }
      case None =>
        // Not needed for non-timestamp-based rollback.
        Future.successful(None)
    }
  }

}
