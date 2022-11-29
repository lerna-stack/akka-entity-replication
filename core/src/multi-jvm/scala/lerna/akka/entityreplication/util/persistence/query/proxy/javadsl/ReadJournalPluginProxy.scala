package lerna.akka.entityreplication.util.persistence.query.proxy.javadsl

import akka.NotUsed
import akka.persistence.query.javadsl._
import akka.persistence.query.{ EventEnvelope, Offset }
import akka.stream.javadsl.Source
import lerna.akka.entityreplication.util.persistence.query.proxy.scaladsl.{
  ReadJournalPluginProxy => ScalaInMemoryReadJournal,
}

class ReadJournalPluginProxy(journal: ScalaInMemoryReadJournal)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery {

  override def currentPersistenceIds(): Source[String, NotUsed] =
    journal.currentPersistenceIds().asJava

  override def persistenceIds(): Source[String, NotUsed] =
    journal.persistenceIds().asJava

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
  ): Source[EventEnvelope, NotUsed] =
    journal.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
  ): Source[EventEnvelope, NotUsed] =
    journal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    journal.currentEventsByTag(tag, offset).asJava

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    journal.eventsByTag(tag, offset).asJava
}
