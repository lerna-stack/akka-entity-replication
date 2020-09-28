package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }

final case class SuspendEntity(shardId: NormalizedShardId, entityId: NormalizedEntityId, stopMessage: Any)
    extends ShardRequest
