package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntryIndex }
import lerna.akka.entityreplication.typed.internal.behavior.Ready.ReadyState
import lerna.akka.entityreplication.typed.internal.effect.SideEffect

import scala.collection.immutable

private[entityreplication] object WaitForReplication {

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
      state: WaitForReplicationState[State],
  ): Behavior[EntityCommand] =
    new WaitForReplication[Command, Event, State](setup).createBehavior(state)

  final case class WaitForReplicationState[State](
      processingCommand: RaftProtocol.ProcessCommand,
      entityState: State,
      lastAppliedLogIndex: LogEntryIndex,
      sideEffects: immutable.Seq[SideEffect[State]],
  )
}

private[entityreplication] class WaitForReplication[Command, Event, State](
    protected val setup: BehaviorSetup[Command, Event, State],
) extends ReplicationOperations[Command, Event, State] {

  import WaitForReplication._

  private[this] type BehaviorState = WaitForReplicationState[State]

  def createBehavior(state: BehaviorState): Behavior[EntityCommand] =
    Behaviors
      .receiveMessage[EntityCommand] {
        case command: RaftProtocol.Replica              => receiveReplica(command, state)
        case command: RaftProtocol.ReplicationSucceeded => receiveReplicationSucceeded(command, state)
        case RaftProtocol.ReplicationFailed             => Ready.behavior(setup, transformReadyState(state)) // Discard side effects
        case command: RaftProtocol.TakeSnapshot         => receiveTakeSnapshot(command, state.entityState)
        case command: RaftProtocol.ProcessCommand =>
          setup.stashBuffer.stash(command)
          Behaviors.same
        case _: RaftProtocol.Activate      => Behaviors.unhandled
        case _: RaftProtocol.ApplySnapshot => Behaviors.unhandled
        case _: RaftProtocol.RecoveryState => Behaviors.unhandled
        case RaftProtocol.RecoveryTimeout  => Behaviors.unhandled
      }.receiveSignal(setup.onSignal(state.entityState))

  private[this] def receiveReplica(command: RaftProtocol.Replica, state: BehaviorState): Behavior[EntityCommand] = {
    // ReplicatedEntityBehavior can receive Replica message when RaftActor demoted to Follower while replicating an event
    Ready.behavior(
      setup,
      transformReadyState(state).applyEvent(setup, command.logEntry.event.event, command.logEntry.index),
    )
  }

  private[this] def receiveReplicationSucceeded(
      command: RaftProtocol.ReplicationSucceeded,
      state: BehaviorState,
  ): Behavior[EntityCommand] = {
    require(
      command.instanceId.nonEmpty,
      "ReplicationSucceeded received by the Entity should contain a instanceId",
      // Entity sends a Replicate command which contains the instanceId
    )
    if (command.instanceId.contains(setup.instanceId)) {
      val event    = EntityEvent(Option(setup.replicationId.entityId), command.event)
      val newState = transformReadyState(state).applyEvent(setup, event.event, command.logEntryIndex)
      applySideEffects(
        state.processingCommand,
        state.sideEffects,
        newState.entityState,
        Ready.behavior(setup, newState),
      )
    } else {
      // ignore ReplicationSucceeded which is produced by replicate command of old ReplicatedEntityBehavior instance
      Behaviors.same
    }
  }

  private[this] def transformReadyState(state: BehaviorState): ReadyState[State] = {
    ReadyState(state.entityState, state.lastAppliedLogIndex)
  }
}
