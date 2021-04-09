package lerna.akka.entityreplication.typed

import akka.actor.typed.ActorRef

class ReplicatedEntityContext[M](
    val entityTypeKey: ReplicatedEntityTypeKey[M],
    val entityId: String,
    val shard: ActorRef[ClusterReplication.ShardCommand],
)
