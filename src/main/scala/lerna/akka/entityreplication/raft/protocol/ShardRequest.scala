package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.model.NormalizedShardId

trait ShardRequest {
  def shardId: NormalizedShardId
}
