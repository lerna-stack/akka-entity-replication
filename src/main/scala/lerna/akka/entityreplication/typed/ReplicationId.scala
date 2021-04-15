package lerna.akka.entityreplication.typed

import lerna.akka.entityreplication.model.NormalizedEntityId

object ReplicationId {

  def apply[Command](entityTypeKey: ReplicatedEntityTypeKey[Command], entityId: String): ReplicationId[Command] = ???
}

trait ReplicationId[Command] {

  def entityTypeKey: ReplicatedEntityTypeKey[Command]

  def entityId: NormalizedEntityId
}
