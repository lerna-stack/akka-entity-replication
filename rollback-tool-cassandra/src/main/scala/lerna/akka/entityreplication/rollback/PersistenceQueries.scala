package lerna.akka.entityreplication.rollback

import akka.NotUsed
import akka.persistence.query.Offset
import akka.stream.scaladsl.Source

import scala.concurrent.Future

private object PersistenceQueries {

  /** Event envelope with tags */
  final case class TaggedEventEnvelope(
      persistenceId: String,
      sequenceNr: SequenceNr,
      event: Any,
      offset: Offset,
      timestamp: Long,
      tags: Set[String],
      writerUuid: String,
  )

}

/** Provides Persistence Queries for rollback */
private trait PersistenceQueries {
  import PersistenceQueries._

  /** Finds the highest sequence number after the given sequence number inclusive
    *
    * If there are no events whose sequence numbers are greater than or equal to the given sequence number, this method
    * returns `Future.successful(None)`.
    */
  def findHighestSequenceNrAfter(
      persistenceId: String,
      from: SequenceNr,
  ): Future[Option[SequenceNr]]

  /** Returns a `Source` that emits current events after the given sequence number inclusive
    *
    * The source will emit events in ascending order of sequence number.
    */
  def currentEventsAfter(
      persistenceId: String,
      from: SequenceNr,
  ): Source[TaggedEventEnvelope, NotUsed]

  /** Returns a `Source` that emits current events before the given sequence number inclusive
    *
    * The source will emit events in descending order of sequence number.
    */
  def currentEventsBefore(
      persistenceId: String,
      from: SequenceNr,
  ): Source[TaggedEventEnvelope, NotUsed]

}
