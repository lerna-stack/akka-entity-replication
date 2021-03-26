package lerna.akka.entityreplication.raft.eventsourced

import lerna.akka.entityreplication.raft.model.LogEntryIndex

object CommitLogStore {
  type ReplicationId = String
}

trait CommitLogStore {
  private[raft] def save(
      replicationId: CommitLogStore.ReplicationId,
      index: LogEntryIndex,
      committedEvent: Any,
  ): Unit
}
