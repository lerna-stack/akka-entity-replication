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

  override def stateName: String = "Inactive"

  def createBehavior(): Behavior[EntityCommand] =
    Behaviors
      .receiveMessage[EntityCommand] {
        case command: RaftProtocol.Activate =>
          receiveActivate(command)
        case command: RaftProtocol.ProcessCommand =>
          if (setup.context.log.isTraceEnabled) {
            setup.context.log.trace(
              "[{}] Stashing ProcessCommand: commandType=[{}]",
              stateName,
              command.command.getClass.getName,
            )
          }
          setup.stashBuffer.stash(command)
          Behaviors.same
        case command: RaftProtocol.Replica =>
          if (setup.context.log.isTraceEnabled) {
            setup.context.log.trace(
              "[{}] Stashing Replica: index=[{}], term=[{}], entityId=[{}], eventType=[{}]",
              stateName,
              command.logEntry.index,
              command.logEntry.term.term,
              command.logEntry.event.entityId.map(_.raw),
              command.logEntry.event.event.getClass.getName,
            )
          }
          setup.stashBuffer.stash(command)
          Behaviors.same
        case command: RaftProtocol.TakeSnapshot =>
          if (setup.context.log.isTraceEnabled) {
            setup.context.log.trace(
              "[{}] Stashing TakeSnapshot: index=[{}], entityId=[{}], replyTo=[{}]",
              stateName,
              command.metadata.logEntryIndex,
              command.metadata.entityId.raw,
              command.replyTo,
            )
          }
          setup.stashBuffer.stash(command)
          Behaviors.same
        case _: RaftProtocol.ApplySnapshot        => Behaviors.unhandled
        case _: RaftProtocol.RecoveryState        => Behaviors.unhandled
        case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
        case RaftProtocol.RecoveryTimeout         => Behaviors.unhandled
        case RaftProtocol.ReplicationFailed       => Behaviors.unhandled
      }.receiveSignal(setup.onSignal(setup.emptyState))

  def receiveActivate(command: RaftProtocol.Activate): Behavior[EntityCommand] = {
    if (setup.context.log.isTraceEnabled) {
      setup.context.log.trace(
        "[{}] Received Activate: recoveryIndex=[{}], shardSnapshotStore=[{}]",
        stateName,
        command.recoveryIndex,
        command.shardSnapshotStore,
      )
    }
    Recovering.behavior(setup, command.shardSnapshotStore, command.recoveryIndex)
  }
}
