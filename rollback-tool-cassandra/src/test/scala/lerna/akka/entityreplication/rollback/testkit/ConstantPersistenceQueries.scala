package lerna.akka.entityreplication.rollback.testkit

import akka.NotUsed
import akka.stream.scaladsl.Source
import lerna.akka.entityreplication.rollback.{ PersistenceQueries, SequenceNr }

import scala.concurrent.Future

/** [[PersistenceQueries]] for the constant envelope source */
private[rollback] final class ConstantPersistenceQueries(
    eventEnvelopes: IndexedSeq[PersistenceQueries.TaggedEventEnvelope],
) extends PersistenceQueries {

  // Requirements:
  require(
    eventEnvelopes.map(_.persistenceId).toSet.sizeIs <= 1,
    "All event envelopes should have the same persistence ID, but they didn't. " +
    s"Persistence IDs were [${eventEnvelopes.map(_.persistenceId).toSet}].",
  )
  for (i <- 1 until eventEnvelopes.size) {
    require(
      eventEnvelopes(i).sequenceNr >= eventEnvelopes(i - 1).sequenceNr,
      s"eventEnvelopes($i).sequenceNr [${eventEnvelopes(i).sequenceNr.value}] should be " +
      s"greater than or equal to eventEnvelopes(${i - 1}).sequenceNr [${eventEnvelopes(i - 1).sequenceNr.value}]",
    )
  }

  /** @inheritdoc */
  override def findHighestSequenceNrAfter(persistenceId: String, from: SequenceNr): Future[Option[SequenceNr]] = {
    Future.successful(
      eventEnvelopes
        .filter(_.persistenceId == persistenceId)
        .lastOption
        .map(_.sequenceNr)
        .filter(_ >= from),
    )
  }

  /** @inheritdoc */
  override def currentEventsAfter(
      persistenceId: String,
      from: SequenceNr,
  ): Source[PersistenceQueries.TaggedEventEnvelope, NotUsed] = {
    Source(eventEnvelopes)
      .filter(_.persistenceId == persistenceId)
      .dropWhile(_.sequenceNr < from)
  }

  /** @inheritdoc */
  override def currentEventsBefore(
      persistenceId: String,
      from: SequenceNr,
  ): Source[PersistenceQueries.TaggedEventEnvelope, NotUsed] = {
    Source(eventEnvelopes.reverse)
      .filter(_.persistenceId == persistenceId)
      .dropWhile(_.sequenceNr > from)
  }

}
