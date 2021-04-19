package lerna.akka.entityreplication.typed.internal

import akka.actor.typed.Behavior
import lerna.akka.entityreplication.typed.{ ReplicatedEntity, ReplicatedEntityContext, ReplicatedEntityTypeKey }

private[entityreplication] final case class ReplicatedEntityImpl[M, E](
    typeKey: ReplicatedEntityTypeKey[M],
    createBehavior: ReplicatedEntityContext[M] => Behavior[M],
) extends ReplicatedEntity[M, E]
