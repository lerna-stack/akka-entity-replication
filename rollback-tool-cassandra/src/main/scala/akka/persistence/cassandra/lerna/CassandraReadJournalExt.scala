package akka.persistence.cassandra.lerna

import akka.NotUsed
import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.TimeBucket
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import akka.stream.scaladsl.Source

import java.util.UUID

object CassandraReadJournalExt {

  final case class CassandraEventEnvelope(
      persistenceId: String,
      sequenceNr: Long,
      timeBucket: Long,
      timestamp: UUID,
      tagPidSequenceNr: Long,
  )

  /** Creates a [[CassandraReadJournalExt]] from the given system and config path
    *
    * It resolves a config at the given path from the system's config. The resolved config should have the same structure
    * as the one of Akka Persistence Cassandra plugin (`akka.persistence.cassandra`).
    */
  def apply(system: ActorSystem, configPath: String): CassandraReadJournalExt = {
    val config   = system.settings.config.getConfig(configPath)
    val queries  = new CassandraReadJournal(system.asInstanceOf[ExtendedActorSystem], config, configPath)
    val settings = new PluginSettings(system, config)
    new CassandraReadJournalExt(queries, settings)
  }

}

/** Provides Akka Persistence Cassandra Queries ([[akka.persistence.cassandra.query.scaladsl.CassandraReadJournal]])
  *
  * Since it depends on internal APIs of Akka Persistence Cassandra, this class is under namespace `akka.persistence.cassandra`.
  */
final class CassandraReadJournalExt private (
    queries: CassandraReadJournal,
    settings: PluginSettings,
) {
  import CassandraReadJournalExt._

  /** Returns an event source that emits events with the given tag
    *
    * It's an alternative version of the public API [[akka.persistence.cassandra.query.scaladsl.CassandraReadJournal.currentEventsByTag]].
    * This method returns some internal information (`timebucket` and `tag_pid_sequence_nr`) that the public API doesn't provide.
    */
  def currentEventsByTag(tag: String, offset: Offset): Source[CassandraEventEnvelope, NotUsed] = {
    queries.currentEventsByTagInternal(tag, offset).map { repr =>
      CassandraEventEnvelope(
        repr.persistentRepr.persistenceId,
        repr.persistentRepr.sequenceNr,
        TimeBucket(repr.offset, settings.eventsByTagSettings.bucketSize).key,
        repr.offset,
        repr.tagPidSequenceNr,
      )
    }
  }

}
