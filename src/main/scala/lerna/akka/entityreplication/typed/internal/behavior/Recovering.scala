package lerna.akka.entityreplication.typed.internal.behavior
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.EntityCommand
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex }
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

  override def stateName: String = "Recovering"

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

      if (context.log.isTraceEnabled) {
        context.log.trace(
          "[{}] Sending FetchSnapshot: entityId=[{}], to=[{}]",
          stateName,
          setup.replicationId.entityId.raw,
          shardSnapshotStore,
        )
      }
      shardSnapshotStore ! SnapshotProtocol.FetchSnapshot(
        setup.replicationId.entityId,
        replyTo = fetchSnapshotResponseMapper.toClassic,
      )

      Behaviors.withTimers { scheduler =>
        if (context.log.isTraceEnabled) {
          context.log.trace(
            "[{}] Starting single RecoveryTimeoutTimer: delay=[{}]",
            stateName,
            setup.settings.recoveryEntityTimeout,
          )
        }
        scheduler.startSingleTimer(
          RecoveryTimeoutTimer,
          RaftProtocol.RecoveryTimeout,
          setup.settings.recoveryEntityTimeout,
        )
        Behaviors
          .receiveMessage[EntityCommand] {
            case command: RaftProtocol.ApplySnapshot =>
              if (context.log.isTraceEnabled) {
                context.log.trace(
                  "[{}] Received ApplySnapshot: index=[{}], entityId=[{}], stateType=[{}]",
                  stateName,
                  command.entitySnapshot.map(_.metadata.logEntryIndex),
                  command.entitySnapshot.map(_.metadata.entityId.raw),
                  command.entitySnapshot.map(_.state.underlying.getClass.getName),
                )
              }
              val snapshotIndex = command.entitySnapshot match {
                case Some(snapshot) => snapshot.metadata.logEntryIndex
                case None           => LogEntryIndex.initial()
              }
              if (context.log.isTraceEnabled) {
                context.log.trace(
                  "[{}] Sending FetchEntityEvents: entityId=[{}], fromIndex=[{}], toIndex=[{}], replyTo=[{}], to=[{}]",
                  stateName,
                  setup.replicationId.entityId.raw,
                  snapshotIndex.next(),
                  recoveryIndex,
                  fetchEntityEventsResponseMapper(command.entitySnapshot),
                  setup.shard,
                )
              }
              setup.shard ! FetchEntityEvents(
                setup.replicationId.entityId,
                from = snapshotIndex.next(),
                to = recoveryIndex,
                fetchEntityEventsResponseMapper(command.entitySnapshot),
              )
              Behaviors.same
            case command: RaftProtocol.RecoveryState =>
              if (context.log.isTraceEnabled) {
                def toLogMessage(logEntry: LogEntry): String = {
                  val entityId  = logEntry.event.entityId.map(_.raw)
                  val eventType = logEntry.event.event.getClass.getName
                  s"index=${logEntry.index}, term=${logEntry.term.term}, entityId=$entityId, eventType=$eventType"
                }
                context.log.trace(
                  "[{}] Received RecoveryState: " +
                  "snapshot.index=[{}], snapshot.entityId=[{}], snapshot.stateType=[{}], " +
                  "events.size=[{}], events.head=[{}], events.last=[{}]",
                  stateName,
                  command.snapshot.map(_.metadata.logEntryIndex),
                  command.snapshot.map(_.metadata.entityId.raw),
                  command.snapshot.map(_.state.underlying.getClass),
                  command.events.size,
                  command.events.headOption.map(toLogMessage),
                  command.events.lastOption.map(toLogMessage),
                )
              }
              scheduler.cancel(RecoveryTimeoutTimer)
              receiveRecoveryState(command)
            case RaftProtocol.RecoveryTimeout =>
              if (context.log.isInfoEnabled)
                context.log.info(
                  "[{}] Entity (name: [{}]) recovering timed out. It will be retried later.",
                  stateName,
                  setup.entityContext.entityId,
                )
              // TODO: Enable backoff to prevent cascade failures
              throw RaftProtocol.EntityRecoveryTimeoutException(context.self.path)
            case command: RaftProtocol.ProcessCommand =>
              if (context.log.isTraceEnabled) {
                context.log.trace(
                  "[{}] Stashing ProcessCommand: commandType=[{}]",
                  stateName,
                  command.command.getClass.getName,
                )
              }
              setup.stashBuffer.stash(command)
              Behaviors.same
            case command: RaftProtocol.Replica =>
              if (context.log.isTraceEnabled) {
                context.log.trace(
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
              if (context.log.isTraceEnabled) {
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
            case _: RaftProtocol.Activate             => Behaviors.unhandled
            case _: RaftProtocol.ReplicationSucceeded => Behaviors.unhandled
            case RaftProtocol.ReplicationFailed       => Behaviors.unhandled
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
    if (context.log.isTraceEnabled) {
      context.log.trace(
        "[{}] Recovering with initial state: index=[{}], stateType=[{}]",
        stateName,
        snapshotAppliedState.lastAppliedLogEntryIndex,
        snapshotAppliedState.entityState.getClass.getName,
      )
    }
    val eventAppliedState =
      command.events.foldLeft(snapshotAppliedState)((state, entry) =>
        state.applyEvent(setup, entry.event.event, entry.index),
      )
    if (context.log.isTraceEnabled) {
      context.log.trace(
        "[{}] Recovered with state: index=[{}], stateType=[{}]",
        stateName,
        eventAppliedState.lastAppliedLogEntryIndex,
        eventAppliedState.entityState.getClass.getName,
      )
    }
    Ready.behavior(setup, eventAppliedState)
  }
}
