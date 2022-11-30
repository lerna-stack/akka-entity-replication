package lerna.akka.entityreplication.rollback

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftActor.{ AppendedEvent, CompactionCompleted, SnapshotSyncCompleted }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.rollback.setup.RaftActorId

import scala.concurrent.Future

/** Provides Persistence Queries for Raft shard */
private class RaftShardPersistenceQueries(
    queries: PersistenceQueries,
) {

  /** Returns a `Source` that emits all involved entity IDs after the given RaftActor's sequence number inclusive
    *
    * Note that the source might emit the same entity ID more than once.
    *
    * The following events contain involved entities:
    *  - [[lerna.akka.entityreplication.raft.RaftActor.AppendedEvent]]
    */
  def entityIdsAfter(
      raftActorId: RaftActorId,
      from: SequenceNr,
  ): Source[NormalizedEntityId, NotUsed] = {
    queries
      .currentEventsAfter(raftActorId.persistenceId, from)
      .map(_.event)
      .collect {
        case appendedEvent: AppendedEvent =>
          appendedEvent.event.entityId
      }
      .mapConcat(_.toSeq)
  }

  /** Finds the last `LogEntryIndex` with which the given `RaftActor` has truncated its `ReplicatedLog` entries
    *
    * The following events indicates that the RaftActor truncates its ReplicatedLog entries:
    *  - [[lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted]]
    *  - [[lerna.akka.entityreplication.raft.RaftActor.SnapshotSyncCompleted]]
    */
  def findLastTruncatedLogEntryIndex(
      raftActorId: RaftActorId,
      from: SequenceNr,
  )(implicit materializer: Materializer): Future[Option[LogEntryIndex]] = {
    queries
      .currentEventsBefore(raftActorId.persistenceId, from)
      .map(_.event)
      .collect {
        case compactionCompleted: CompactionCompleted =>
          compactionCompleted.snapshotLastLogIndex
        case snapshotSyncCompleted: SnapshotSyncCompleted =>
          snapshotSyncCompleted.snapshotLastLogIndex
      }
      .runWith(Sink.headOption)
  }

}
