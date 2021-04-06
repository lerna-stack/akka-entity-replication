package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.model.NormalizedEntityId

private[entityreplication] final case class EntityEvent(entityId: Option[NormalizedEntityId], event: Any)
