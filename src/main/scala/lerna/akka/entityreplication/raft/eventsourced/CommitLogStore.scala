package lerna.akka.entityreplication.raft.eventsourced

import akka.actor.ActorRef
import lerna.akka.entityreplication.model.NormalizedShardId
import lerna.akka.entityreplication.raft.model.LogEntryIndex

private[entityreplication] trait CommitLogStore {
  private[raft] def save(
      shardId: NormalizedShardId,
      index: LogEntryIndex,
      committedEvent: Any,
  ): Unit

  def actorRef: ActorRef

}
