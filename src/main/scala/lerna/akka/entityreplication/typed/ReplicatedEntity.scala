package lerna.akka.entityreplication.typed

import akka.actor.typed.Behavior
import lerna.akka.entityreplication.typed.internal.ReplicatedEntityImpl

object ReplicatedEntity {

  /**
    * Creates a [[ReplicatedEntity]].
    */
  def apply[M](typeKey: ReplicatedEntityTypeKey[M])(
      createBehavior: ReplicatedEntityContext[M] => Behavior[M],
  ): ReplicatedEntity[M, ReplicationEnvelope[M]] = ReplicatedEntityImpl(typeKey, createBehavior)
}

/**
  * Defines how the entity should be created. Used in [[ClusterReplication.init()]].
  */
trait ReplicatedEntity[M, E] {

  def createBehavior: ReplicatedEntityContext[M] => Behavior[M]

  def typeKey: ReplicatedEntityTypeKey[M]
}
