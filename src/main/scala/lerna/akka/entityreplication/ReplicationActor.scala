package lerna.akka.entityreplication

import akka.actor.{ ActorLogging, ActorPath, ActorRef, Cancellable, Stash, Status }
import akka.pattern.extended.ask
import akka.pattern.{ pipe, AskTimeoutException }
import akka.util.Timeout
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex, NoOp }
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

object ReplicationActor {
  final case class TakeSnapshot(metadata: EntitySnapshotMetadata, replyTo: ActorRef)
  final case class Snapshot(metadata: EntitySnapshotMetadata, state: EntityState)

  private final case object RecoveryTimeout

  final case class EntityRecoveryTimeoutException(entityPath: ActorPath) extends RuntimeException

  final case class ReplicationFailure()
}

trait ReplicationActor[StateData] extends akka.lerna.Actor with ActorLogging with Stash with akka.lerna.StashFactory {
  import ReplicationActor._
  import context.dispatcher

  private val internalStash = createStash()

  private[this] val settings = ClusterReplicationSettings(context.system)

  override def receive: Receive = receiveCommand

  private[this] sealed trait State {
    def stateReceive(receive: Receive, message: Any): Unit
  }

  override def aroundPreStart(): Unit = {
    super.aroundPreStart()
    context.parent ! RequestRecovery(NormalizedEntityId.of(self.path))
  }

  private[this] val recovering: State = new State {

    private[this] val recoveryTimeoutTimer: Cancellable =
      context.system.scheduler.scheduleOnce(settings.recoveryEnittyTimeout, self, RecoveryTimeout)

    override def stateReceive(receive: Receive, message: Any): Unit =
      message match {
        case RecoveryTimeout =>
          // to restart
          // TODO: BackoffSupervisor を使ってカスケード障害を回避する
          throw EntityRecoveryTimeoutException(self.path)

        case RecoveryState(logEntries, maybeSnapshot) =>
          recoveryTimeoutTimer.cancel()
          maybeSnapshot.foreach { snapshot =>
            innerApplyEvent(
              SnapshotOffer(snapshot.state.underlying),
              snapshot.metadata.logEntryIndex,
            )
          }
          logEntries.foreach { logEntry =>
            innerApplyEvent(logEntry.event.event, logEntry.index)
          }
          changeState(ready)
          internalStash.unstashAll()
        case _ =>
          internalStash.stash()
      }
  }

  private[this] val ready: State = new State {

    override def stateReceive(receive: Receive, message: Any): Unit =
      message match {
        case Command(command) =>
          receive.applyOrElse[Any, Unit](
            command,
            command => {
              log.warning("unhandled {} by receiveCommand", command)
            },
          )

        case Replica(logEntry) =>
          innerApplyEvent(logEntry.event.event, logEntry.index)

        case TakeSnapshot(metadata, replyTo) =>
          replyTo ! Snapshot(metadata, EntityState(currentState))

        case other => ReplicationActor.super.aroundReceive(receive, other)
      }
  }

  private[this] def waitForReplicationResponse[A](event: A, handler: A => Unit): State =
    new State {

      private[this] var replicatedLogEntries: Seq[LogEntry] = Seq()

      override def stateReceive(receive: Receive, message: Any): Unit =
        message match {
          case Replica(logEntry) =>
            replicatedLogEntries :+= logEntry
          case ReplicationSucceeded(_, logEntryIndex) =>
            changeState(ready)
            replicatedLogEntries.foreach { logEntry =>
              innerApplyEvent(logEntry.event.event, logEntry.index)
            }
            replicatedLogEntries = Seq()
            internalStash.unstashAll()
            handler(event)
            lastAppliedLogEntryIndex = logEntryIndex
          case msg: ReplicationFailed =>
            // TODO: 実装
            log.warning("ReplicationFailed: {}", msg)
          case Status.Failure(_: AskTimeoutException) =>
            changeState(ready)
            log.warning("replication timeout")
            self ! ReplicationFailure()
          case msg: Status.Failure =>
            // TODO: 実装
            log.warning("Status.Failure: {}", msg)
          case TakeSnapshot(metadata, replyTo) =>
            replyTo ! Snapshot(metadata, EntityState(currentState))
          case _ => internalStash.stash()
        }
    }

  def receiveReplica: Receive

  def receiveCommand: Receive

  def currentState: StateData

  private[this] var replicationState: State         = recovering
  private[this] def changeState(state: State): Unit = replicationState = state

  override def aroundReceive(receive: Receive, msg: Any): Unit =
    replicationState.stateReceive(receive, msg)

  private[this] val replicationConfig = context.system.settings.config.getConfig("lerna.akka.entityreplication")

  import util.JavaDurationConverters._
  private[this] val replicationTimeout: Timeout = Timeout(replicationConfig.getDuration("replication-timeout").asScala)

  def replicate[A](event: A)(handler: A => Unit): Unit = {
    val originalSender = sender()
    changeState(waitForReplicationResponse(event, handler))
    import context.dispatcher
    context.parent
      .ask(replyTo => Replicate(event, replyTo, NormalizedEntityId.of(self.path)))(replicationTimeout).pipeTo(self)(
        originalSender,
      )
  }

  def ensureConsistency(handler: => Unit): Unit = replicate(NoOp)(_ => (handler _)())

  /**
    * 最後に適用されたイベントの [[LogEntryIndex]] を記録し、既に適用済みのイベントが再度連携された場合は無視する。
    * [[ReplicationActor]] から [[RequestRecovery]] を要求するタイミングと [[RaftActor]] がコミットするタイミングが重なったときに重複が発生する。
    */
  private[this] var lastAppliedLogEntryIndex: LogEntryIndex = LogEntryIndex.initial()

  private[this] def innerApplyEvent(event: Any, logEntryIndex: LogEntryIndex): Unit = {
    if (logEntryIndex > lastAppliedLogEntryIndex) {
      receiveReplica.applyOrElse[Any, Unit](
        event,
        event => {
          log.warning("unhandled {} by receiveReplica", event)
        },
      )
      lastAppliedLogEntryIndex = logEntryIndex
    }
  }
}
