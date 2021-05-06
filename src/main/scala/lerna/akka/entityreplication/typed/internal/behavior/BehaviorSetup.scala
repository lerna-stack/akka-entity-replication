package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, Behavior, Signal }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.typed.{ ClusterReplication, ReplicatedEntityBehavior, ReplicatedEntityContext }
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.typed.internal.ReplicationId

private[entityreplication] final case class BehaviorSetup[Command, Event, State](
    entityContext: ReplicatedEntityContext[Command],
    emptyState: State,
    commandHandler: ReplicatedEntityBehavior.CommandHandler[Command, Event, State],
    eventHandler: ReplicatedEntityBehavior.EventHandler[State, Event],
    signalHandler: PartialFunction[(State, Signal), Unit],
    stopMessage: Option[Command],
    replicationId: ReplicationId[Command],
    shard: ActorRef[ClusterReplication.ShardCommand],
    settings: ClusterReplicationSettings,
    context: ActorContext[EntityCommand],
) {

  def onSignal(state: State): PartialFunction[(ActorContext[EntityCommand], Signal), Behavior[EntityCommand]] = {
    case (_, signal) if signalHandler.isDefinedAt((state, signal)) =>
      signalHandler((state, signal))
      Behaviors.same
    case _ =>
      Behaviors.unhandled
  }
}
