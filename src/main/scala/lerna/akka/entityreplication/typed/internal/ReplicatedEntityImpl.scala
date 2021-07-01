package lerna.akka.entityreplication.typed.internal

import akka.actor.typed.Behavior
import lerna.akka.entityreplication.typed.{
  ClusterReplicationSettings,
  ReplicatedEntity,
  ReplicatedEntityContext,
  ReplicatedEntityTypeKey,
}

private[entityreplication] final case class ReplicatedEntityImpl[M, E](
    typeKey: ReplicatedEntityTypeKey[M],
    createBehavior: ReplicatedEntityContext[M] => Behavior[M],
    settings: Option[ClusterReplicationSettings] = None,
) extends ReplicatedEntity[M, E] {

  override def withSettings(settings: ClusterReplicationSettings): ReplicatedEntity[M, E] =
    copy(settings = Option(settings))
}
