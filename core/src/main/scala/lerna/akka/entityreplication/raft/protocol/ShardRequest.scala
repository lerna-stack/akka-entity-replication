package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.model.NormalizedShardId

private[entityreplication] trait ShardRequest {
  def shardId: NormalizedShardId
}
