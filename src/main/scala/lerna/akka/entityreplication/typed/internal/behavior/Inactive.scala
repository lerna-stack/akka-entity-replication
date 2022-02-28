package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand

private[entityreplication] object Inactive {

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
  ): Behavior[EntityCommand] = {
    new Inactive[Command, Event, State](setup).createBehavior()
  }
}

private[entityreplication] class Inactive[Command, Event, State](
    protected val setup: BehaviorSetup[Command, Event, State],
) extends ReplicationOperations[Command, Event, State] {

  def createBehavior(): Behavior[EntityCommand] =
    Behaviors
      .receiveMessage[EntityCommand] {
        case command: RaftProtocol.Activate =>
          receiveActivate(command)
        case command: RaftProtocol.ProcessCommand =>
          setup.stashBuffer.stash(command)
          Behaviors.same
        case command: RaftProtocol.Replica =>
          setup.stashBuffer.stash(command)
          Behaviors.same
        case command: RaftProtocol.TakeSnapshot =>
          setup.stashBuffer.stash(command)
          Behaviors.same
        case _: RaftProtocol.ApplySnapshot        => Behaviors.unhandled
        case _: RaftProtocol.RecoveryState        => Behaviors.unhandled
        case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
        case RaftProtocol.RecoveryTimeout         => Behaviors.unhandled
        case RaftProtocol.ReplicationFailed       => Behaviors.unhandled
      }.receiveSignal(setup.onSignal(setup.emptyState))

  def receiveActivate(command: RaftProtocol.Activate): Behavior[EntityCommand] = {
    Recovering.behavior(setup, command.shardSnapshotStore, command.recoveryIndex)
  }
}
