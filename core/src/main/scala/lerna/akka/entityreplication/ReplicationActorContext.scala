package lerna.akka.entityreplication

import akka.actor.ActorRef

private[entityreplication] class ReplicationActorContext(val entityId: String, val shard: ActorRef)
