package lerna.akka.entityreplication.raft.persistence

import lerna.akka.entityreplication.model.NormalizedShardId
import lerna.akka.entityreplication.raft.routing.MemberIndex

private[entityreplication] final case class CompactionCompletedTag(
    memberIndex: MemberIndex,
    shardId: NormalizedShardId,
) {
  private[this] val delimiter = ":"

  override def toString: String = s"CompactionCompleted${delimiter}${shardId.underlying}${delimiter}${memberIndex.role}"
}
