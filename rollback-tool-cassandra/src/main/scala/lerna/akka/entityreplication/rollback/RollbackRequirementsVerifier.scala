package lerna.akka.entityreplication.rollback

import akka.Done

import java.time.Instant
import scala.concurrent.Future

/** Verifies the rollback request meets rollback requirements */
private trait RollbackRequirementsVerifier {

  /** Verifies that the given rollback request for `persistenceId` meets rollback requirements
    *
    *  - If the request meets requirements, this returns a successful `Future`.
    *  - Otherwise, this returns a failed `Future` containing a [[RollbackRequirementsNotFulfilled]].
    *
    * @param toSequenceNr
    *   - `None` means deleting all data for rollback.
    *   - `Some(sequenceNr)` means rollback to `sequenceNr`.
    * @param toTimestampHintOpt
    *   - `None` means the request is not timestamp-based.
    *   - `Some(_)` means the request is timestamp-based.
    */
  def verify(
      persistenceId: String,
      toSequenceNr: Option[SequenceNr],
      toTimestampHintOpt: Option[Instant],
  ): Future[Done]

}
