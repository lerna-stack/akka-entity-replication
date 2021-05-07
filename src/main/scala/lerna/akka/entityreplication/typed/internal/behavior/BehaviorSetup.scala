package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.scaladsl.{ ActorContext, Behaviors, StashBuffer }
import akka.actor.typed.{ ActorRef, Behavior, Signal }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.EntityInstanceId
import lerna.akka.entityreplication.typed.{ ClusterReplication, ReplicatedEntityBehavior, ReplicatedEntityContext }
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.typed.internal.ReplicationId

private[entityreplication] final class BehaviorSetup[Command, Event, State](
    val entityContext: ReplicatedEntityContext[Command],
    val emptyState: State,
    val commandHandler: ReplicatedEntityBehavior.CommandHandler[Command, Event, State],
    val eventHandler: ReplicatedEntityBehavior.EventHandler[State, Event],
    val signalHandler: PartialFunction[(State, Signal), Unit],
    val stopMessage: Option[Command],
    val replicationId: ReplicationId[Command],
    val shard: ActorRef[ClusterReplication.ShardCommand],
    val settings: ClusterReplicationSettings,
    val context: ActorContext[EntityCommand],
    val instanceId: EntityInstanceId,
    val stashBuffer: StashBuffer[EntityCommand],
) {

  def onSignal(state: State): PartialFunction[(ActorContext[EntityCommand], Signal), Behavior[EntityCommand]] = {
    case (_, signal) if signalHandler.isDefinedAt((state, signal)) =>
      signalHandler((state, signal))
      Behaviors.same
  }
}
