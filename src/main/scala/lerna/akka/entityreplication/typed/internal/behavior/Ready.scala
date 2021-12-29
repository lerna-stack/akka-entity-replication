package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }
import lerna.akka.entityreplication.typed.internal.effect._
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.typed.internal.behavior.WaitForReplication.WaitForReplicationState

private[entityreplication] object Ready {

  final case class ReadyState[State](
      entityState: State,
      /**
        * It records the [[LogEntryIndex]] of the last applied event, and ignores
        * if the entity receives again an already applied event.
        *
        * Duplication occurs when the timing of requesting [[RequestRecovery]] from [[ReplicationActor]]
        * and the timing of committing the event by [[RaftActor]] overlap.
        */
      lastAppliedLogEntryIndex: LogEntryIndex,
  ) {

    def applyEvent[Command, Event](
        setup: BehaviorSetup[Command, Event, State],
        event: Any,
        logEntryIndex: LogEntryIndex,
    ): ReadyState[State] = {
      if (logEntryIndex > lastAppliedLogEntryIndex) {
        event match {
          case NoOp =>
            copy(
              lastAppliedLogEntryIndex = logEntryIndex,
            )
          case event =>
            copy(
              entityState = setup.eventHandler(entityState, event.asInstanceOf[Event]),
              lastAppliedLogEntryIndex = logEntryIndex,
            )
        }
      } else this
    }
  }

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
      readyState: ReadyState[State],
  ): Behavior[EntityCommand] = {
    setup.stashBuffer.unstashAll {
      Behaviors.setup { context =>
        new Ready(setup, context).createBehavior(readyState)
      }
    }
  }
}

private[entityreplication] class Ready[Command, Event, State](
    val setup: BehaviorSetup[Command, Event, State],
    context: ActorContext[EntityCommand],
) extends ReplicationOperations[Command, Event, State] {

  import Ready._

  private[this] type BehaviorState = ReadyState[State]

  def createBehavior(readyState: BehaviorState): Behavior[EntityCommand] =
    Behaviors
      .receiveMessage[EntityCommand] {
        case command: RaftProtocol.ProcessCommand => receiveProcessCommand(command, readyState)
        case command: RaftProtocol.Replica        => receiveReplica(command, readyState)
        case command: RaftProtocol.TakeSnapshot   => receiveTakeSnapshot(command, readyState.entityState)
        case _: RaftProtocol.Activate             => Behaviors.unhandled
        case _: RaftProtocol.ApplySnapshot        => Behaviors.unhandled
        case _: RaftProtocol.RecoveryState        => Behaviors.unhandled
        case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
        case RaftProtocol.RecoveryTimeout         => Behaviors.unhandled
      }.receiveSignal(setup.onSignal(readyState.entityState))

  def receiveProcessCommand(command: RaftProtocol.ProcessCommand, state: BehaviorState): Behavior[EntityCommand] = {
    val effect = setup.commandHandler(state.entityState, command.command.asInstanceOf[Command])
    effect match {
      case effect: EffectImpl[Event, State] =>
        applyEffect(command, effect, state)
      case other =>
        throw new IllegalStateException(s"Illegal effect Implementation: ${other.getClass.getName}")
    }
  }

  def receiveReplica(command: RaftProtocol.Replica, state: BehaviorState): Behavior[EntityCommand] = {
    val logEntry = command.logEntry
    createBehavior(state.applyEvent(setup, logEntry.event.event, logEntry.index))
  }

  def applyEffect(
      command: RaftProtocol.ProcessCommand,
      effect: EffectImpl[Event, State],
      state: BehaviorState,
  ): Behavior[EntityCommand] = {
    effect.mainEffect match {
      case ReplicateEffect(event)    => replicate(command, event, state, effect)
      case EnsureConsistencyEffect() => replicate(command, NoOp, state, effect)
      case ReplicateNothingEffect() =>
        applySideEffects(command, effect.sideEffects, state.entityState, Behaviors.same)
      case StashEffect() =>
        setup.stashBuffer.stash(command)
        applySideEffects(command, effect.sideEffects, state.entityState, Behaviors.same)
    }
  }

  private[this] def replicate(
      command: RaftProtocol.ProcessCommand,
      event: Any,
      state: BehaviorState,
      effect: EffectImpl[Event, State],
  ): Behavior[EntityCommand] = {
    setup.shard ! RaftProtocol.Replicate(
      event = event,
      replyTo = context.self.toClassic,
      entityId = setup.replicationId.entityId,
      instanceId = setup.instanceId,
      originSender = context.system.deadLetters.toClassic, // typed API can not use sender
    )
    WaitForReplication.behavior(
      setup,
      WaitForReplicationState(
        command,
        state.entityState,
        state.lastAppliedLogEntryIndex,
        effect.sideEffects,
      ),
    )
  }

}
