package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors, StashBuffer }
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }
import lerna.akka.entityreplication.typed.internal.effect._
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.model.EntityInstanceId
import lerna.akka.entityreplication.typed.internal.behavior.WaitForReplication.WaitForReplicationState

private[entityreplication] object Ready {

  final case class ReadyState[State](
      entityState: State,
      instanceId: InstanceId,
      lastAppliedLogEntryIndex: LogEntryIndex,
      stashBuffer: StashBuffer[EntityCommand],
  ) {

    def applyEvent[Command, Event](
        setup: BehaviorSetup[Command, Event, State],
        event: Any,
        logEntryIndex: LogEntryIndex,
    ): ReadyState[State] =
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
  }

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
      readyState: ReadyState[State],
  ): Behavior[EntityCommand] =
    Behaviors.setup { context =>
      new Ready(setup, context).createBehavior(readyState)
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
      .receiveMessage {
        case command: RaftProtocol.ProcessCommand => receiveProcessCommand(command, readyState)
        case command: RaftProtocol.Replica        => receiveReplica(command, readyState)
        case command: RaftProtocol.TakeSnapshot   => receiveTakeSnapshot(command, readyState.entityState)
        case _: RaftProtocol.RecoveryState        => Behaviors.unhandled
        case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
        case RaftProtocol.RecoveryTimeout         => Behaviors.unhandled
      }

  def receiveProcessCommand(command: RaftProtocol.ProcessCommand, state: BehaviorState): Behavior[EntityCommand] = {
    val effect = setup.commandHandler(state.entityState, command.command.asInstanceOf[Command])
    effect match {
      case effect: EffectImpl[Event, State] =>
        applyEffect(command, effect, state)
      case other =>
        throw new IllegalStateException(s"Illegal effect Implementation: ${other.getClass.getName}") // TODO:
    }
  }

  def receiveReplica(command: RaftProtocol.Replica, state: BehaviorState): Behavior[EntityCommand] = {
    val logEntry = command.logEntry
    createBehavior(state.applyEvent(setup, logEntry.event.event, logEntry.index))
  }

  def applyEffect(
      command: EntityCommand,
      effect: EffectImpl[Event, State],
      state: BehaviorState,
  ): Behavior[EntityCommand] = {
    effect.mainEffect match {
      case ReplicateEffect(event)    => replicate(event, state, effect)
      case EnsureConsistencyEffect() => replicate(NoOp, state, effect)
      case ReplicateNothingEffect() =>
        applySideEffects(effect.sideEffects, state.stashBuffer, state.entityState, Behaviors.same)
      case StashEffect() =>
        state.stashBuffer.stash(command)
        applySideEffects(effect.sideEffects, state.stashBuffer, state.entityState, Behaviors.same)
    }
  }

  private[this] def replicate(
      event: Any,
      state: BehaviorState,
      effect: EffectImpl[Event, State],
  ): Behavior[EntityCommand] = {
    setup.shard ! RaftProtocol.Replicate(
      event = event,
      replyTo = context.self.toClassic,
      entityId = setup.replicationId.entityId,
      instanceId = EntityInstanceId(state.instanceId.underlying),
      originSender = context.system.deadLetters.toClassic, // typed API can not use sender
    )
    WaitForReplication.behavior(
      setup,
      WaitForReplicationState(
        state.entityState,
        state.instanceId,
        state.lastAppliedLogEntryIndex,
        effect.sideEffects,
        state.stashBuffer,
      ),
    )
  }

}
