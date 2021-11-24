package lerna.akka.entityreplication.typed.internal.behavior
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.typed.internal.behavior.Ready.ReadyState

private[entityreplication] object Recovering {

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
  ): Behavior[EntityCommand] = {
    new Recovering[Command, Event, State](setup).createBehavior()
  }

  final case object RecoveryTimeoutTimer
}

private[entityreplication] class Recovering[Command, Event, State](
    protected val setup: BehaviorSetup[Command, Event, State],
) extends ReplicationOperations[Command, Event, State] {

  import Recovering._

  def createBehavior(): Behavior[EntityCommand] =
    Behaviors.setup { context =>
      setup.shard ! RaftProtocol.RequestRecovery(setup.replicationId.entityId)

      Behaviors.withTimers { scheduler =>
        scheduler.startSingleTimer(
          RecoveryTimeoutTimer,
          RaftProtocol.RecoveryTimeout,
          setup.settings.recoveryEntityTimeout,
        )
        Behaviors
          .receiveMessage[EntityCommand] {
            case command: RaftProtocol.RecoveryState =>
              scheduler.cancel(RecoveryTimeoutTimer)
              receiveRecoveryState(command)
            case RaftProtocol.RecoveryTimeout =>
              if (context.log.isInfoEnabled) context.log.info(
                "Entity (name: {}) recovering timed out. It will be retried later.",
                setup.entityContext.entityId,
              )
              // TODO: Enable backoff to prevent cascade failures
              throw RaftProtocol.EntityRecoveryTimeoutException(context.self.path)
            case command: RaftProtocol.ProcessCommand =>
              setup.stashBuffer.stash(command)
              Behaviors.same
            case command: RaftProtocol.Replica =>
              setup.stashBuffer.stash(command)
              Behaviors.same
            case command: RaftProtocol.TakeSnapshot =>
              setup.stashBuffer.stash(command)
              Behaviors.same
            case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
          }.receiveSignal(setup.onSignal(setup.emptyState))
      }
    }

  private[this] def receiveRecoveryState(
      command: RaftProtocol.RecoveryState,
  ): Behavior[EntityCommand] = {
    val (entityState, lastAppliedLogIndex) = command.snapshot.fold(
      ifEmpty = (setup.emptyState, LogEntryIndex.initial()),
    ) { snapshot =>
      (snapshot.state.underlying.asInstanceOf[State], snapshot.metadata.logEntryIndex)
    }
    val snapshotAppliedState =
      ReadyState(entityState, lastAppliedLogIndex)
    val eventAppliedState =
      command.events.foldLeft(snapshotAppliedState)((state, entry) =>
        state.applyEvent(setup, entry.event.event, entry.index),
      )
    Ready.behavior(setup, eventAppliedState)
  }
}
