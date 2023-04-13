package lerna.akka.entityreplication.rollback.cassandra

import akka.Done
import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import akka.event.{ Logging, LoggingAdapter }
import akka.persistence.cassandra.lerna.CassandraReadJournalExt
import akka.persistence.cassandra.lerna.CassandraReadJournalExt.CassandraEventEnvelope
import akka.persistence.query.Offset
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSessionRegistry }
import akka.stream.scaladsl.Sink
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Statement }
import lerna.akka.entityreplication.rollback.SequenceNr

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }

private object DeleteTagViews {

  private final case class TagWriteProgress(tagPidSequenceNr: Long, sequenceNr: Long, offset: UUID)

  private final case class TagScanning(sequenceNr: Long)

  private final case class TagViewMetadata(progress: TagWriteProgress, scanning: TagScanning)

  private final case class DeleteTagViewElement(
      envelope: Option[CassandraEventEnvelope],
      lastMetadata: Option[TagViewMetadata],
  )

}

private final class DeleteTagViews(
    systemProvider: ClassicActorSystemProvider,
    settings: CassandraPersistentActorRollbackSettings,
) {
  import DeleteTagViews._

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  private implicit val executionContext: ExecutionContext =
    system.dispatcher

  private val queries: CassandraReadJournalExt =
    CassandraReadJournalExt(system, settings.pluginLocation)

  private lazy val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor(settings.pluginLocation)
  private lazy val statements: CassandraPersistentActorRollbackStatements =
    new CassandraPersistentActorRollbackStatements(settings)
  private lazy val deleteTagViews: Future[PreparedStatement] =
    session.prepare(statements.journal.deleteTagViews)
  private lazy val insertTagWriteProgress: Future[PreparedStatement] =
    session.prepare(statements.journal.insertTagWriteProgress)
  private lazy val insertTagScanning: Future[PreparedStatement] =
    session.prepare(statements.journal.insertTagScanning)
  private lazy val deleteTagWriteProgress: Future[PreparedStatement] =
    session.prepare(statements.journal.deleteTagWriteProgress)
  private lazy val deleteTagScanning: Future[PreparedStatement] =
    session.prepare(statements.journal.deleteTagScanning)

  private val log: LoggingAdapter =
    Logging(system, getClass)

  /** Deletes `tag_views` after the given sequence number (inclusive) for the persistence ID and tag
    *
    * It also updates (or deletes) metadata (`tag_write_progress`, `tag_scanning`) for `tag_views`.
    */
  def execute(persistenceId: String, tag: String, from: SequenceNr): Future[Done] = {
    if (log.isDebugEnabled) {
      log.debug(
        "Deleting tag_views, tag_write_progress, and tag_scanning: persistence_id=[{}], tag=[{}], from=[{}]",
        persistenceId,
        tag,
        from.value,
      )
    }
    val deletionFuture = queries
      .currentEventsByTag(tag, Offset.noOffset)
      .filter(_.persistenceId == persistenceId)
      .statefulMapConcat { () =>
        // Collecting the last (after the deletion completes) metadata for updating tag_write_progress and tag_scanning.
        var lastMetadata: Option[TagViewMetadata] = None

        { envelope =>
          if (envelope.sequenceNr < from.value) {
            val progress = TagWriteProgress(envelope.tagPidSequenceNr, envelope.sequenceNr, envelope.timestamp)
            val scanning = TagScanning(envelope.sequenceNr)
            lastMetadata.foreach { metadata =>
              assert(
                metadata.progress.tagPidSequenceNr < envelope.tagPidSequenceNr,
                s"tag_pid_sequence_nr of tagged events (persistence_id=[$persistenceId], tag=[$tag]) should always increase, but decreased:" +
                s" [${metadata.progress.tagPidSequenceNr}] -> [${envelope.tagPidSequenceNr}]",
              )
              assert(
                metadata.progress.sequenceNr < envelope.sequenceNr,
                s"sequence_nr of tagged events (persistence_id=[$persistenceId], tag=[$tag}) should always increase, but decreased:" +
                s" [${metadata.progress.sequenceNr}] -> [${envelope.sequenceNr}]",
              )
            }
            lastMetadata = Option(TagViewMetadata(progress, scanning))
            Seq(DeleteTagViewElement(None, lastMetadata))
          } else {
            Seq(DeleteTagViewElement(Option(envelope), lastMetadata))
          }
        }
      }
      .mapAsync(1) { element =>
        element.envelope match {
          case Some(envelope) =>
            deleteTagView(tag, envelope).map(_ => element.lastMetadata)
          case None =>
            Future.successful(element.lastMetadata)
        }
      }
      .runWith(Sink.lastOption)
      .map(_.flatten)
      .flatMap { lastMetadata =>
        lastMetadata.foreach { metadata =>
          assert(
            metadata.progress.sequenceNr < from.value,
            s"sequence_nr [${metadata.progress.sequenceNr}] of tag_view_progress (persistence_id=[$persistenceId], tag=[$tag])" +
            s" should always be less than from_sequence_nr [${from.value}]",
          )
          assert(
            metadata.scanning.sequenceNr < from.value,
            s"sequence_nr [${metadata.scanning.sequenceNr}] of tag_scanning (persistence_id=[$persistenceId], tag=[$tag])" +
            s" should always be less than from_sequence_nr [${from.value}]",
          )
        }
        updateTagViewMetadata(persistenceId, tag, lastMetadata)
      }
    deletionFuture.foreach { _ =>
      if (log.isDebugEnabled) {
        log.debug(
          "Deleted tag_views, tag_write_progress, and tag_scanning: persistence_id=[{}], tag=[{}], from=[{}]",
          persistenceId,
          tag,
          from.value,
        )
      }
    }
    deletionFuture
  }

  private def updateTagViewMetadata(
      persistenceId: String,
      tag: String,
      lastMetadata: Option[TagViewMetadata],
  ): Future[Done] = {
    // There is a subtle timing at which multiple DeleteTagView with the same persistence ID but different tags update
    // (or delete) the tag_scanning table. It might be a race condition, which make a tag recovery of the persistent
    // actor inefficient but doesn't break data consistency. The sequence number of tag_scanning table after deletions
    // (or updates) should be less than `from` sequence number. A higher sequence number as possible is great for the
    // efficiency of the tag recovery but is not required.
    lastMetadata match {
      case Some(metadata) =>
        val progressUpdate = updateTagWriteProgress(persistenceId, tag, metadata.progress)
        val scanningUpdate = updateTagScanning(persistenceId, metadata.scanning)
        Future.sequence(Seq(progressUpdate, scanningUpdate)).map(_ => Done)
      case None =>
        val progressDeletion = deleteTagWriteProgress(persistenceId, tag)
        val scanningDeletion = deleteTagScanning(persistenceId)
        Future.sequence(Seq(progressDeletion, scanningDeletion)).map(_ => Done)
    }
  }

  private def deleteTagView(
      tag: String,
      envelope: CassandraEventEnvelope,
  ): Future[Done] = {
    if (log.isDebugEnabled) {
      log.debug(
        s"Deleting tag_view: tag=[$tag], timebucket=[{}], timestamp=[{}], persistenceId=[{}], tag_pid_sequence_nr=[{}]",
        envelope.timeBucket,
        envelope.timestamp,
        envelope.persistenceId,
        envelope.tagPidSequenceNr,
      )
    }
    deleteTagViews.flatMap { ps =>
      executeWrite(
        ps.bind(tag, envelope.timeBucket, envelope.timestamp, envelope.persistenceId, envelope.tagPidSequenceNr),
      )
    }
  }

  private def updateTagWriteProgress(
      persistenceId: String,
      tag: String,
      progress: TagWriteProgress,
  ): Future[Done] = {
    if (log.isDebugEnabled) {
      log.debug(
        s"Updating tag_write_progress: persistence_id=[$persistenceId], tag=[{}], sequence_nr=[{}], tag_pid_sequence_nr=[{}], offset=[{}]",
        tag,
        progress.sequenceNr,
        progress.tagPidSequenceNr,
        progress.offset,
      )
    }
    insertTagWriteProgress.flatMap { ps =>
      executeWrite(ps.bind(persistenceId, tag, progress.sequenceNr, progress.tagPidSequenceNr, progress.offset))
    }
  }

  private def updateTagScanning(persistenceId: String, scanning: TagScanning): Future[Done] = {
    if (log.isDebugEnabled) {
      log.debug("Updating tag_scanning: persistence_id=[{}], sequence_nr=[{}]", persistenceId, scanning.sequenceNr)
    }
    insertTagScanning.flatMap { ps =>
      executeWrite(ps.bind(persistenceId, scanning.sequenceNr))
    }
  }

  private def deleteTagWriteProgress(persistenceId: String, tag: String): Future[Done] = {
    if (log.isDebugEnabled) {
      log.debug("Deleting tag_write_progress: persistence_id=[{}], tag=[{}]", persistenceId, tag)
    }
    deleteTagWriteProgress.flatMap { ps =>
      executeWrite(ps.bind(persistenceId, tag))
    }
  }

  private def deleteTagScanning(persistenceId: String): Future[Done] = {
    if (log.isDebugEnabled) {
      log.debug("Deleting tag_scanning: persistence_id=[{}]", persistenceId)
    }
    deleteTagScanning.flatMap { ps =>
      executeWrite(ps.bind(persistenceId))
    }
  }

  private def executeWrite[T <: Statement[T]](statement: Statement[T]): Future[Done] = {
    require(!settings.dryRun, s"Write query [$statement] is not allowed in dry-run mode")
    session.executeWrite(statement.setExecutionProfileName(settings.journal.writeProfile))
  }

}
