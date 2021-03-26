package lerna.akka.entityreplication.raft.eventsourced

import lerna.akka.entityreplication.model.NormalizedShardId
import lerna.akka.entityreplication.raft.model.LogEntryIndex

trait CommitLogStore {
  private[raft] def save(
      shardId: NormalizedShardId,
      index: LogEntryIndex,
      committedEvent: Any,
  ): Unit
}
