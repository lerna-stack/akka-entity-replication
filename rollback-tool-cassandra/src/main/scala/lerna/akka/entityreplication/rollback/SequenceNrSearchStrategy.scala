package lerna.akka.entityreplication.rollback

import java.time.Instant
import scala.concurrent.Future

/** Searches for the sequence number from events such that it meets timestamp requirement */
private trait SequenceNrSearchStrategy {

  /** Returns the highest sequence number from events whose timestamp is less than or equal to the given one */
  def findUpperBound(persistenceId: String, timestamp: Instant): Future[Option[SequenceNr]]

}
