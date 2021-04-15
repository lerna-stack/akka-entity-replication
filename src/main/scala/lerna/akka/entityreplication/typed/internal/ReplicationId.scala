package lerna.akka.entityreplication.typed.internal

import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.typed.ReplicatedEntityTypeKey

private[entityreplication] object ReplicationId {

  def apply[Command](entityTypeKey: ReplicatedEntityTypeKey[Command], entityId: String): ReplicationId[Command] = ???
}

trait ReplicationId[Command] {

  def entityTypeKey: ReplicatedEntityTypeKey[Command]

  def entityId: NormalizedEntityId
}
