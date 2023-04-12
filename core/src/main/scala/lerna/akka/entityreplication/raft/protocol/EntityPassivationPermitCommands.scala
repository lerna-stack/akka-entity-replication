package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }

/** Non-leader RaftActors (followers or candidates) will send this message to the leader RaftActor if it want to passivate
  * the entity with `entityId`. The leader should reply with [[EntityPassivationPermitResponse]] to the sender RaftActor.
  */
private[entityreplication] final case class EntityPassivationPermitRequest(
    shardId: NormalizedShardId,
    entityId: NormalizedEntityId,
    stopMessage: Any,
) extends ShardRequest
    with ClusterReplicationSerializable

/** The leader RaftActor will send this message back to the sender RaftActor (follower or candidate). */
private[entityreplication] sealed trait EntityPassivationPermitResponse extends ClusterReplicationSerializable

/** The leader RaftActor will send this message back to the sender RaftActor (follower or candidate) if it permits the
  * passivation request ([[EntityPassivationPermitRequest]]).
  */
private[entityreplication] final case class EntityPassivationPermitted(
    entityId: NormalizedEntityId,
    stopMessage: Any,
) extends EntityPassivationPermitResponse

/** The leader RaftActor will send this message back to the sender RaftActor (follower or candidate) if it denies the
  * passivation request ([[EntityPassivationPermitRequest]]).
  */
private[entityreplication] final case class EntityPassivationDenied(
    entityId: NormalizedEntityId,
) extends EntityPassivationPermitResponse
