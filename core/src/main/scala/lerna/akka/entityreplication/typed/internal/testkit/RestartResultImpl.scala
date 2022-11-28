package lerna.akka.entityreplication.typed.internal.testkit

import lerna.akka.entityreplication.typed.testkit.ReplicatedEntityBehaviorTestKit.RestartResult

private[entityreplication] final case class RestartResultImpl[State](state: State) extends RestartResult[State]
