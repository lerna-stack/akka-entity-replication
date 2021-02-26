package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }

final case class TryCreateEntity(shardId: NormalizedShardId, entityId: NormalizedEntityId)
    extends ShardRequest
    with ClusterReplicationSerializable
