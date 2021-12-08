package lerna.akka.entityreplication

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ Actor, Cancellable, Stash }
import akka.event.Logging
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId }
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

private[entityreplication] object ReplicationActor {

  private[this] val instanceIdCounter = new AtomicInteger(1)

  private def generateInstanceId(): EntityInstanceId = EntityInstanceId(instanceIdCounter.getAndIncrement())
}

@deprecated(message = "Use typed.ReplicatedEntityBehavior instead", since = "2.0.0")
trait ReplicationActor[StateData] extends Actor with Stash with akka.lerna.StashFactory {
  import context.dispatcher

  private val internalStash = createStash()

  private val instanceId = ReplicationActor.generateInstanceId()

  private[this] val settings = ClusterReplicationSettings.create(context.system)

  private[this] val log = Logging(context.system, this)

  override def receive: Receive = receiveCommand

  private[this] sealed trait State {
    def stateReceive(receive: Receive, message: Any): Unit
  }

  override def aroundPreStart(): Unit = {
    super.aroundPreStart()
    requestRecovery()
  }

  override def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.aroundPreRestart(reason, message)
    requestRecovery()
  }

  private[this] def requestRecovery(): Unit = {
    context.parent ! RequestRecovery(NormalizedEntityId.of(self.path))
  }

  private[this] val recovering: State = new State {

    private[this] val recoveryTimeoutTimer: Cancellable =
      context.system.scheduler.scheduleOnce(settings.recoveryEntityTimeout, self, RecoveryTimeout)

    override def stateReceive(receive: Receive, message: Any): Unit =
      message match {
        case RecoveryTimeout =>
          // to restart
          // TODO: BackoffSupervisor を使ってカスケード障害を回避する
          if (log.isInfoEnabled)
            log.info("Entity (name: {}) recovering timed out. It will be retried later.", self.path.name)
          throw EntityRecoveryTimeoutException(self.path)

        case RecoveryState(logEntries, maybeSnapshot) =>
          println(s"RecoveryState ${maybeSnapshot} - ${logEntries}")
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
        case ProcessCommand(command) =>
          receive.applyOrElse[Any, Unit](
            command,
            command => {
              if (log.isWarningEnabled) log.warning("unhandled {} by receiveCommand", command)
            },
          )

        case Replica(logEntry) =>
          println(s"replica: ${logEntry}")
          innerApplyEvent(logEntry.event.event, logEntry.index)

        case TakeSnapshot(metadata, replyTo) =>
          println(s"take snapshot: ${currentState}")
          replyTo ! Snapshot(metadata, EntityState(currentState))

        case other => ReplicationActor.super.aroundReceive(receive, other)
      }
  }

  private[this] def waitForReplicationResponse[A](event: A, handler: A => Unit): State =
    new State {

      override def stateReceive(receive: Receive, message: Any): Unit =
        message match {
          case Replica(logEntry) =>
            // ReplicationActor can receive Replica message when RaftActor demoted to Follower while replicating an event
            innerApplyEvent(logEntry.event.event, logEntry.index)
            changeState(ready)
            internalStash.unstashAll()
          case ReplicationSucceeded(_, logEntryIndex, responseInstanceId) if responseInstanceId.contains(instanceId) =>
            changeState(ready)
            internalStash.unstashAll()
            handler(event)
            lastAppliedLogEntryIndex = logEntryIndex
          case _: ReplicationSucceeded =>
          // ignore ReplicationSucceeded which is produced by replicate command of old ReplicationActor instance
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

  def replicate[A](event: A)(handler: A => Unit): Unit = {
    changeState(waitForReplicationResponse(event, handler))
    context.parent ! Replicate(
      event,
      replyTo = self,
      NormalizedEntityId.of(self.path),
      instanceId,
      originSender = sender(),
    )
  }

  def ensureConsistency(handler: => Unit): Unit = replicate(NoOp)(_ => handler)

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
          if (log.isWarningEnabled) log.warning("unhandled {} by receiveReplica", event)
        },
      )
      lastAppliedLogEntryIndex = logEntryIndex
    }
  }
}
