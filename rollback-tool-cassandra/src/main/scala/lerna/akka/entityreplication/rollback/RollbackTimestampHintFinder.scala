package lerna.akka.entityreplication.rollback

import lerna.akka.entityreplication.rollback.RollbackTimestampHintFinder.TimestampHint

import java.time.Instant
import scala.concurrent.Future

private object RollbackTimestampHintFinder {

  /** Timestamp hint for rollback
    *
    * Timestamp-based rollback for the persistent actor with `persistenceId` requires a rollback timestamp new than or
    * equal to `timestamp` of data (event or snapshot) with `sequenceNr`. While the timestamp is a hint and might need
    * to be more strictly correct, it helps to know why the timestamp-based rollback request doesn't fulfill rollback
    * requirements.
    */
  final case class TimestampHint(
      persistenceId: String,
      sequenceNr: SequenceNr,
      timestamp: Instant,
  )

}

/** Finds a rollback timestamp hint for the requirements */
private trait RollbackTimestampHintFinder {

  /** Finds a rollback timestamp hint for the given rollback requirements
    *
    * If a hint is not found, this method returns a failed `Future` containing a [[RollbackTimestampHintNotFound]].
    *
    * @see [[RollbackTimestampHintFinder.TimestampHint]]
    */
  def findTimestampHint(requirements: PersistentActorRollback.RollbackRequirements): Future[TimestampHint]

}
