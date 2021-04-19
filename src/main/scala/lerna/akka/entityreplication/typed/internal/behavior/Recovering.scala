package lerna.akka.entityreplication.typed.internal.behavior
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors, StashBuffer }
import akka.lerna.StashBuffer
import lerna.akka.entityreplication.model.EntityInstanceId
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.typed.internal.behavior.Ready.ReadyState

private[entityreplication] object Recovering {

  object RecoveringState {
    def initial[State](context: ActorContext[EntityCommand], instanceId: EntityInstanceId): RecoveringState[State] =
      RecoveringState(instanceId, StashBuffer(context, Int.MaxValue /* TODO: Should I get from config? */ ))
  }

  final case class RecoveringState[State](
      instanceId: EntityInstanceId,
      stashBuffer: StashBuffer[EntityCommand],
  )

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
      recoveringState: RecoveringState[State],
  ): Behavior[EntityCommand] = {
    new Recovering[Command, Event, State](setup).createBehavior(recoveringState)
  }

  final case object RecoveryTimeoutTimer
}

private[entityreplication] class Recovering[Command, Event, State](
    protected val setup: BehaviorSetup[Command, Event, State],
) extends ReplicationOperations[Command, Event, State] {

  import Recovering._

  private[this] type BehaviorState = RecoveringState[State]

  def createBehavior(state: BehaviorState): Behavior[EntityCommand] =
    Behaviors.setup { context =>
      setup.shard ! RaftProtocol.RequestRecovery(setup.replicationId.entityId)

      Behaviors.withTimers { scheduler =>
        scheduler.startSingleTimer(
          RecoveryTimeoutTimer,
          RaftProtocol.RecoveryTimeout,
          setup.settings.recoveryEntityTimeout,
        )
        Behaviors.receiveMessage {
          case command: RaftProtocol.RecoveryState =>
            scheduler.cancel(RecoveryTimeoutTimer)
            receiveRecoveryState(command, state)
          case RaftProtocol.RecoveryTimeout =>
            context.log.info(
              "Entity (name: {}) recovering timed out. It will be retried later.",
              setup.entityContext.entityId,
            )
            // TODO: Enable backoff to prevent cascade failures
            throw RaftProtocol.EntityRecoveryTimeoutException(context.self.path)
          case command: RaftProtocol.ProcessCommand =>
            state.stashBuffer.stash(command)
            Behaviors.same
          case command: RaftProtocol.Replica =>
            state.stashBuffer.stash(command)
            Behaviors.same
          case command: RaftProtocol.TakeSnapshot =>
            state.stashBuffer.stash(command)
            Behaviors.same
          case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
        }
      }
    }

  private[this] def receiveRecoveryState(
      command: RaftProtocol.RecoveryState,
      state: BehaviorState,
  ): Behavior[EntityCommand] = {
    val (entityState, lastAppliedLogIndex) = command.snapshot.fold(
      ifEmpty = (setup.emptyState, LogEntryIndex.initial()),
    ) { snapshot =>
      (snapshot.state.underlying.asInstanceOf[State], snapshot.metadata.logEntryIndex)
    }
    val snapshotAppliedState =
      ReadyState(entityState, state.instanceId, lastAppliedLogIndex, state.stashBuffer)
    val eventAppliedState =
      command.events.foldLeft(snapshotAppliedState)((state, entry) =>
        state.applyEvent(setup, entry.event.event, entry.index),
      )
    Ready.behavior(setup, eventAppliedState)
  }
}
