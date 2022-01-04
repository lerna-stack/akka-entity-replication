package lerna.akka.entityreplication.typed.internal.behavior
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshot
import lerna.akka.entityreplication.typed.internal.behavior.Ready.ReadyState

private[entityreplication] object Recovering {

  def behavior[Command, Event, State](
      setup: BehaviorSetup[Command, Event, State],
      shardSnapshotStore: ActorRef[SnapshotProtocol.Command],
      recoveryIndex: LogEntryIndex,
  ): Behavior[EntityCommand] = {
    new Recovering[Command, Event, State](setup).createBehavior(shardSnapshotStore, recoveryIndex)
  }

  final case object RecoveryTimeoutTimer
}

private[entityreplication] class Recovering[Command, Event, State](
    protected val setup: BehaviorSetup[Command, Event, State],
) extends ReplicationOperations[Command, Event, State] {

  import Recovering._

  def createBehavior(
      shardSnapshotStore: ActorRef[SnapshotProtocol.Command],
      recoveryIndex: LogEntryIndex,
  ): Behavior[EntityCommand] =
    Behaviors.setup { context =>
      val fetchSnapshotResponseMapper: ActorRef[SnapshotProtocol.FetchSnapshotResponse] =
        context.messageAdapter {
          case found: SnapshotProtocol.SnapshotFound => RaftProtocol.ApplySnapshot(Option(found.snapshot))
          case _: SnapshotProtocol.SnapshotNotFound  => RaftProtocol.ApplySnapshot(None)
        }

      def fetchEntityEventsResponseMapper(snapshot: Option[EntitySnapshot]): ActorRef[FetchEntityEventsResponse] =
        context.messageAdapter {
          case FetchEntityEventsResponse(events) => RaftProtocol.RecoveryState(events, snapshot)
        }

      shardSnapshotStore ! SnapshotProtocol.FetchSnapshot(
        setup.replicationId.entityId,
        replyTo = fetchSnapshotResponseMapper.toClassic,
      )

      Behaviors.withTimers { scheduler =>
        scheduler.startSingleTimer(
          RecoveryTimeoutTimer,
          RaftProtocol.RecoveryTimeout,
          setup.settings.recoveryEntityTimeout,
        )
        Behaviors
          .receiveMessage[EntityCommand] {
            case command: RaftProtocol.ApplySnapshot =>
              val snapshotIndex = command.entitySnapshot match {
                case Some(snapshot) => snapshot.metadata.logEntryIndex
                case None           => LogEntryIndex.initial()
              }
              setup.shard ! FetchEntityEvents(
                setup.replicationId.entityId,
                from = snapshotIndex.next(),
                to = recoveryIndex,
                fetchEntityEventsResponseMapper(command.entitySnapshot),
              )
              Behaviors.same
            case command: RaftProtocol.RecoveryState =>
              scheduler.cancel(RecoveryTimeoutTimer)
              receiveRecoveryState(command)
            case RaftProtocol.RecoveryTimeout =>
              if (context.log.isInfoEnabled)
                context.log.info(
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
            case _: RaftProtocol.Activate             => Behaviors.unhandled
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
