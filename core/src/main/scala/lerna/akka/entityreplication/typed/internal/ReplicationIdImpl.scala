package lerna.akka.entityreplication.typed.internal

import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.typed.ReplicatedEntityTypeKey

private[entityreplication] final case class ReplicationIdImpl[Command](
    entityTypeKey: ReplicatedEntityTypeKey[Command],
    entityId: NormalizedEntityId,
) extends ReplicationId[Command]
