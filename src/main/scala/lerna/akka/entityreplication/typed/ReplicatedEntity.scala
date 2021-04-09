package lerna.akka.entityreplication.typed

import akka.actor.typed.Behavior

object ReplicatedEntity {

  /**
    * Creates a [[ReplicatedEntity]].
    */
  def apply[M](typeKey: ReplicatedEntityTypeKey[M])(
      createBehavior: ReplicatedEntityContext[M] => Behavior[M],
  ): ReplicatedEntity[M, ReplicationEnvelope[M]] = ???
}

/**
  * Defines how the entity should be created. Used in [[ClusterReplication.init()]].
  */
trait ReplicatedEntity[M, E] {

  def createBehavior: ReplicatedEntityContext[M] => Behavior[M]

  def typeKey: ReplicatedEntityTypeKey[M]
}
