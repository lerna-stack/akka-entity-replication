package lerna.akka.entityreplication.typed

import akka.actor.typed.{ ActorRef, ActorSystem, Extension, ExtensionId }

object ClusterReplication extends ExtensionId[ClusterReplication] {

  override def createExtension(system: ActorSystem[_]): ClusterReplication = ???

  trait ShardCommand
}

/**
  * This extension provides fast recovery by creating replicas of entities in multiple locations
  * and always synchronizing their status.
  */
trait ClusterReplication extends Extension {

  def init[M, E](entity: ReplicatedEntity[M, E]): ActorRef[E]

  /**
    * Create an [[ActorRef]]-like reference to a specific replicated entity.
    */
  def entityRefFor[M](typeKey: ReplicatedEntityTypeKey[M], entityId: String): ReplicatedEntityRef[M]

}
