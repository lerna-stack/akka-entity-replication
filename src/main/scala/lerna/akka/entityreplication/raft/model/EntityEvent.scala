package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.model.NormalizedEntityId

final case class EntityEvent(entityId: Option[NormalizedEntityId], event: Any)
