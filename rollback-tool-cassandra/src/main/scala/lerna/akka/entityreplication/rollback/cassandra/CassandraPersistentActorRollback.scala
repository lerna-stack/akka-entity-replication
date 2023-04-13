package lerna.akka.entityreplication.rollback.cassandra

import akka.Done
import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import akka.event.{ Logging, LoggingAdapter }
import akka.persistence.cassandra.cleanup.Cleanup
import akka.persistence.cassandra.reconciler.Reconciliation
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSessionRegistry }
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Statement }
import lerna.akka.entityreplication.rollback.{ PersistenceQueries, PersistentActorRollback, SequenceNr }

import scala.concurrent.Future

/** @inheritdoc */
private final class CassandraPersistentActorRollback(
    systemProvider: ClassicActorSystemProvider,
    val settings: CassandraPersistentActorRollbackSettings,
) extends PersistentActorRollback {

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  private val log: LoggingAdapter =
    Logging(system, getClass)

  private val cleanup: Cleanup =
    new Cleanup(system, settings.cleanup)

  private val reconciliation: Reconciliation =
    new Reconciliation(system, settings.reconciliation)

  private val deleteTagViews: DeleteTagViews =
    new DeleteTagViews(system, settings)

  private[cassandra] val queries: CassandraPersistenceQueries =
    new CassandraPersistenceQueries(systemProvider, settings.queries)

  private lazy val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor(settings.pluginLocation)
  private lazy val statements: CassandraPersistentActorRollbackStatements =
    new CassandraPersistentActorRollbackStatements(settings)
  private lazy val deleteMessagesFromPreparedStatement: Future[PreparedStatement] =
    session.prepare(statements.journal.deleteMessagesFrom)
  private lazy val deleteSnapshotsFromPreparedStatement: Future[PreparedStatement] =
    session.prepare(statements.snapshot.deleteSnapshotsFrom)

  import system.dispatcher

  if (settings.dryRun) {
    log.info(
      "CassandraPersistentActorRollback (plugin-location = [{}]) is in dry-run mode",
      settings.pluginLocation,
    )
  }

  /** @inheritdoc */
  override def isDryRun: Boolean = settings.dryRun

  /** @inheritdoc */
  override def persistenceQueries: PersistenceQueries = queries

  /** Rolls back events, snapshots, and tag views to the given sequence number for the persistence ID
    *
    * Internally, this method deletes events and snapshots after the given sequence number (exclusive). Furthermore, it
    * deletes tag views entirely and then rebuild them. Note that this method requires that all events are available in
    * the past. This method cannot rebuild tag views if an event has been deleted.
    *
    * If dry-run mode, this method logs INFO messages and does not execute actual writes(deletes,inserts,updates).
    */
  def rollbackTo(persistenceId: String, to: SequenceNr): Future[Done] = {
    if (settings.dryRun) {
      log.info("dry-run: roll back to sequence_nr [{}] for persistenceId [{}]", to.value, persistenceId)
      // NOTE: continue to run the following, which logs info messages describing what action will be executed.
    }
    val deleteFromSequenceNr = to + 1
    for {
      _ <- deleteEventsFrom(persistenceId, deleteFromSequenceNr)
      _ <- deleteSnapshotsFrom(persistenceId, deleteFromSequenceNr)
      _ <- deleteTagView(persistenceId, deleteFromSequenceNr)
      _ <- rebuildTagView(persistenceId)
    } yield Done
  }

  /** Delete all events, snapshots, and tag views for the persistence ID
    *
    * If dry-run mode, this method logs INFO messages and does not execute actual deletes.
    */
  override def deleteAll(persistenceId: String): Future[Done] = {
    if (settings.dryRun) {
      log.info("dry-run: delete for persistence_id [{}]", persistenceId)
      // NOTE: continue to run the following, which logs info messages describing what action will be executed.
    }
    for {
      _ <- cleanup.deleteAllEvents(persistenceId, neverUsePersistenceIdAgain = true)
      _ <- cleanup.deleteAllSnapshots(persistenceId)
      _ <- deleteTagView(persistenceId, SequenceNr(1))
    } yield Done
  }

  /** Delete all events after the given sequence number (inclusive) for the persistence ID
    *
    * If dry-run mode, this method logs an INFO message and does not execute actual deletes.
    *
    * @throws java.lang.IllegalArgumentException if the given sequence number is not greater than 1
    */
  def deleteEventsFrom(persistenceId: String, from: SequenceNr): Future[Done] = {
    require(
      from.value > 1,
      s"from [${from.value}] should be greater than 1",
    )
    def delete(currentPartitionNr: PartitionNr, toPartitionNr: PartitionNr): Future[Done] = {
      if (currentPartitionNr.value > toPartitionNr.value) {
        Future.successful(Done)
      } else {
        deleteMessagesFromPreparedStatement
          .flatMap { ps =>
            if (settings.dryRun) {
              log.info(
                "dry-run: delete events (partition_nr=[{}], sequence_nr >= [{}]) for persistenceId [{}]",
                currentPartitionNr.value,
                from.value,
                persistenceId,
              )
              Future.successful(Done)
            } else {
              executeWrite(ps.bind(persistenceId, currentPartitionNr.value, from.value), settings.journal.writeProfile)
            }
          }.flatMap { _ =>
            delete(currentPartitionNr + 1, toPartitionNr)
          }
      }
    }
    val fromPartitionNr =
      PartitionNr.fromSequenceNr(from, settings.journal.targetPartitionSize)
    val toPartitionNrFuture =
      queries.findHighestPartitionNr(persistenceId, fromPartitionNr).map(_.getOrElse(fromPartitionNr))
    for {
      toPartitionNr <- toPartitionNrFuture
      _             <- delete(fromPartitionNr, toPartitionNr)
    } yield Done
  }

  /** Delete all snapshots after the given sequence number (inclusive) for the persistence ID
    *
    * If dry-run mode, this method logs an INFO message and does not execute actual deletes.
    *
    * @throws java.lang.IllegalArgumentException if the given sequence number is not greater than 1
    */
  def deleteSnapshotsFrom(persistenceId: String, from: SequenceNr): Future[Done] = {
    require(
      from.value > 1,
      s"from [${from.value}] should be greater than 1",
    )
    deleteSnapshotsFromPreparedStatement.flatMap { ps =>
      if (settings.dryRun) {
        log.info(
          "dry-run: delete snapshots (sequence_nr >= [{}]) for persistence_id [{}]",
          from.value,
          persistenceId,
        )
        Future.successful(Done)
      } else {
        executeWrite(ps.bind(persistenceId, from.value), settings.snapshot.writeProfile)
      }
    }
  }

  /** Deletes tag views and metadata (`tag_write_progress`, `tag_scanning`) after the given sequence number (inclusive) for the persistence ID
    *
    * Prerequisite: `tag_write_progress` should be consistent (not broken) since it is used to select all tags for the given
    * persistence ID.
    *
    * If dry-run mode, this method logs an INFO message and does not execute actual deletes.
    */
  def deleteTagView(persistenceId: String, from: SequenceNr): Future[Done] = {
    reconciliation
      .tagsForPersistenceId(persistenceId)
      .flatMap { tags =>
        if (settings.dryRun) {
          log.info(
            "dry-run: delete tag_view(s) [{}] after sequence_nr [{}] for persistence_id [{}] ",
            tags,
            from.value,
            persistenceId,
          )
          Future.successful(Done)
        } else {
          Future.traverse(tags) { tag =>
            deleteTagViews.execute(persistenceId, tag, from)
          }
        }
      }.map(_ => Done)
  }

  /** Rebuilds tag views for the given persistence ID
    *
    * Prerequisites: the tag views (`tag_view`, `tag_write_progress`, `tag_scanning`) should contain no elements for the
    * given persistence ID.
    *
    * If dry-run mode, this method logs an INFO message and does not execute actual writes.
    *
    * This method delegates actual processing to [[akka.persistence.cassandra.reconciler.Reconciliation.rebuildTagViewForPersistenceIds]].
    */
  def rebuildTagView(persistenceId: String): Future[Done] = {
    if (settings.dryRun) {
      log.info("dry-run: rebuild tag_view for persistence_id [{}]", persistenceId)
      Future.successful(Done)
    } else {
      reconciliation.rebuildTagViewForPersistenceIds(persistenceId)
    }
  }

  private def executeWrite[T <: Statement[T]](statement: Statement[T], writeProfileName: String): Future[Done] = {
    require(!settings.dryRun, s"Write query [$statement] is not allowed in dry-run mode")
    session.executeWrite(statement.setExecutionProfileName(writeProfileName))
  }

}
