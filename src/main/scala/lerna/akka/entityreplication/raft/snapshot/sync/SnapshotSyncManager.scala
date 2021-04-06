package lerna.akka.entityreplication.raft.snapshot.sync

import akka.actor.{ ActorLogging, ActorRef, Props, Status }
import akka.pattern.extended.ask
import akka.pattern.pipe
import akka.persistence.{ PersistentActor, SnapshotOffer }
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.persistence.query.scaladsl.CurrentEventsByTagQuery
import akka.stream.{ KillSwitches, UniqueKillSwitch }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.util.Timeout
import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, Term }
import lerna.akka.entityreplication.raft.persistence.CompactionCompletedTag
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata
import lerna.akka.entityreplication.raft.snapshot.{ ShardSnapshotStore, SnapshotProtocol }
import lerna.akka.entityreplication.util.ActorIds

import scala.concurrent.Future

private[entityreplication] object SnapshotSyncManager {

  def props(
      typeName: TypeName,
      srcMemberIndex: MemberIndex,
      dstMemberIndex: MemberIndex,
      dstShardSnapshotStore: ActorRef,
      shardId: NormalizedShardId,
      raftSettings: RaftSettings,
  ): Props =
    Props(
      new SnapshotSyncManager(
        typeName,
        srcMemberIndex,
        dstMemberIndex,
        dstShardSnapshotStore,
        shardId,
        raftSettings,
      ),
    )

  sealed trait Command

  final case class SyncSnapshot(
      srcLatestSnapshotLastLogTerm: Term,
      srcLatestSnapshotLastLogIndex: LogEntryIndex,
      dstLatestSnapshotLastLogTerm: Term,
      dstLatestSnapshotLastLogIndex: LogEntryIndex,
      replyTo: ActorRef,
  ) extends Command

  sealed trait Response

  final case class SyncSnapshotSucceeded(
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      srcMemberIndex: MemberIndex,
  ) extends Response

  final case class SyncSnapshotAlreadySucceeded(
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      srcMemberIndex: MemberIndex,
  ) extends Response

  final case class SyncSnapshotFailed() extends Response

  sealed trait Event

  final case class SyncCompleted(offset: Offset) extends Event with ClusterReplicationSerializable

  sealed trait State

  final case class SyncProgress(offset: Offset) extends State with ClusterReplicationSerializable

  final case class CompactionEnvelope(event: CompactionCompleted, offset: Offset)

  sealed trait SyncStatus

  final case class SyncCompleteAll(snapshotLastLogTerm: Term, snapshotLastLogIndex: LogEntryIndex, offset: Offset)
      extends SyncStatus
  final case class SyncIncomplete() extends SyncStatus

  sealed trait SyncFailures

  final case class SynchronizationAbortException() extends RuntimeException with SyncFailures

  final case class SnapshotNotFoundException(
      typeName: TypeName,
      srcMemberIndex: MemberIndex,
      entityId: NormalizedEntityId,
  ) extends RuntimeException(
        s"Snapshot not found for [entityId: $entityId, typeName: $typeName, memberIndex: $srcMemberIndex]",
      )
      with SyncFailures

  final case class SaveSnapshotFailureException(
      typeName: TypeName,
      dstMemberIndex: MemberIndex,
      metadata: EntitySnapshotMetadata,
  ) extends RuntimeException(
        s"Save snapshot failure for entity (${metadata.entityId}) to [typeName: $typeName, memberIndex: $dstMemberIndex]",
      )
      with SyncFailures

  final case class SnapshotUpdateConflictException(
      typeName: TypeName,
      srcMemberIndex: MemberIndex,
      entityId: NormalizedEntityId,
      expectLogIndex: LogEntryIndex,
      actualLogIndex: LogEntryIndex,
  ) extends RuntimeException(
        s"Newer (logEntryIndex: $actualLogIndex) snapshot found than expected (logEntryIndex: $expectLogIndex) in [typeName: $typeName, memberIndex: $srcMemberIndex, entityId: $entityId]",
      )
      with SyncFailures
}

private[entityreplication] class SnapshotSyncManager(
    typeName: TypeName,
    srcMemberIndex: MemberIndex,
    dstMemberIndex: MemberIndex,
    dstShardSnapshotStore: ActorRef,
    shardId: NormalizedShardId,
    settings: RaftSettings,
) extends PersistentActor
    with ActorLogging {
  import SnapshotSyncManager._

  override def journalPluginId: String = settings.journalPluginId

  override def snapshotPluginId: String = settings.snapshotStorePluginId

  override def persistenceId: String =
    ActorIds.persistenceId(
      "SnapshotSyncManager",
      typeName.underlying,
      srcMemberIndex.role,
      dstMemberIndex.role,
      shardId.underlying,
    )

  private[this] val readJournal =
    PersistenceQuery(context.system)
      .readJournalFor[CurrentEventsByTagQuery](settings.queryPluginId)

  private[this] val sourceShardSnapshotStore =
    context.actorOf(ShardSnapshotStore.props(typeName, settings, srcMemberIndex))

  override def receiveRecover: Receive = {

    case SnapshotOffer(_, snapshot: SyncProgress) =>
      this.state = snapshot

    case event: Event => updateState(event)
  }

  private[this] var state = SyncProgress(Offset.noOffset)

  private[this] var killSwitch: Option[UniqueKillSwitch] = None

  override def receiveCommand: Receive = ready

  def ready: Receive = {

    case SyncSnapshot(
          srcLatestSnapshotLastLogTerm,
          srcLatestSnapshotLastLogIndex,
          dstLatestSnapshotLastLogTerm,
          dstLatestSnapshotLastLogIndex,
          replyTo,
        )
        if srcLatestSnapshotLastLogTerm == dstLatestSnapshotLastLogTerm
        && srcLatestSnapshotLastLogIndex == dstLatestSnapshotLastLogIndex =>
      replyTo ! SyncSnapshotAlreadySucceeded(
        dstLatestSnapshotLastLogTerm,
        dstLatestSnapshotLastLogIndex,
        srcMemberIndex,
      )
      log.info(
        "Snapshot synchronization already completed: " +
        s"(typeName: $typeName, memberIndex: $srcMemberIndex, snapshotLastLogTerm: ${srcLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $srcLatestSnapshotLastLogIndex)" +
        s" -> (typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
      )
      context.stop(self)

    case SyncSnapshot(
          srcLatestSnapshotLastLogTerm,
          srcLatestSnapshotLastLogIndex,
          dstLatestSnapshotLastLogTerm,
          dstLatestSnapshotLastLogIndex,
          replyTo,
        ) =>
      import context.dispatcher
      val (killSwitch, result) = synchronizeSnapshots(
        srcLatestSnapshotLastLogIndex,
        dstLatestSnapshotLastLogTerm,
        dstLatestSnapshotLastLogIndex,
        state.offset,
      )
      this.killSwitch = Option(killSwitch)
      result pipeTo self
      context.become(synchronizing(replyTo, dstLatestSnapshotLastLogTerm, dstLatestSnapshotLastLogIndex))
      log.info(
        "Snapshot synchronization started: " +
        s"(typeName: $typeName, memberIndex: $srcMemberIndex, snapshotLastLogTerm: ${srcLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $srcLatestSnapshotLastLogIndex)" +
        s" -> (typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
      )

    case _: akka.persistence.SaveSnapshotSuccess =>
      context.stop(self)

    case _: akka.persistence.SaveSnapshotFailure =>
      context.stop(self)
  }

  def synchronizing(
      replyTo: ActorRef,
      dstLatestSnapshotLastLogTerm: Term,
      dstLatestSnapshotLastLogIndex: LogEntryIndex,
  ): Receive = {

    case _: SyncSnapshot => // ignore

    case syncStatus: SyncStatus =>
      syncStatus match {
        case completeAll: SyncCompleteAll =>
          this.killSwitch = None
          persist(SyncCompleted(completeAll.offset)) { event =>
            updateState(event)
            saveSnapshot(this.state)
            replyTo ! SyncSnapshotSucceeded(
              completeAll.snapshotLastLogTerm,
              completeAll.snapshotLastLogIndex,
              srcMemberIndex,
            )
            log.info(
              "Snapshot synchronization completed: " +
              s"(typeName: $typeName, memberIndex: $srcMemberIndex)" +
              s" -> (typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
            )
          }
        case _: SyncIncomplete =>
          this.killSwitch = None
          replyTo ! SyncSnapshotFailed()
          log.info(
            "Snapshot synchronization is incomplete: " +
            s"(typeName: $typeName, memberIndex: $srcMemberIndex)" +
            s" -> (typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
          )
          context.stop(self)
      }

    case Status.Failure(e) =>
      this.killSwitch = None
      replyTo ! SyncSnapshotFailed()
      log.warning(
        "Snapshot synchronization aborted: " +
        s"(typeName: $typeName, memberIndex: $srcMemberIndex)" +
        s" -> (typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)" +
        s" cause: $e",
      )
      context.stop(self)

    case _: akka.persistence.SaveSnapshotSuccess => // ignore: previous execution result
    case _: akka.persistence.SaveSnapshotFailure => // ignore: previous execution result
  }

  def updateState(event: Event): Unit =
    event match {
      case SyncCompleted(offset) =>
        this.state = SyncProgress(offset)
        context.become(ready)
    }

  override def postStop(): Unit = {
    try {
      this.killSwitch.foreach { switch =>
        switch.abort(SynchronizationAbortException())
      }
    } finally super.postStop()
  }

  def synchronizeSnapshots(
      srcLatestSnapshotLastLogIndex: LogEntryIndex,
      dstLatestSnapshotLastLogTerm: Term,
      dstLatestSnapshotLastLogIndex: LogEntryIndex,
      offset: Offset,
  ): (UniqueKillSwitch, Future[SyncStatus]) = {

    import context.system
    import context.dispatcher
    implicit val timeout: Timeout = Timeout(settings.snapshotSyncPersistenceOperationTimeout)

    readJournal
      .currentEventsByTag(CompactionCompletedTag(srcMemberIndex, shardId).toString, offset)
      .viaMat(KillSwitches.single)(Keep.right)
      .collect {
        case EventEnvelope(offset, _, _, event: CompactionCompleted)
            if dstLatestSnapshotLastLogTerm <= event.snapshotLastLogTerm && dstLatestSnapshotLastLogIndex < event.snapshotLastLogIndex =>
          CompactionEnvelope(event, offset)
      }
      .flatMapConcat { envelope =>
        Source(envelope.event.entityIds)
          .mapAsync(settings.snapshotSyncCopyingParallelism) { entityId =>
            for {
              fetchSnapshotResult <- {
                ask(sourceShardSnapshotStore, replyTo => SnapshotProtocol.FetchSnapshot(entityId, replyTo))
                  .mapTo[SnapshotProtocol.FetchSnapshotResponse]
                  .flatMap {
                    case response: SnapshotProtocol.SnapshotFound
                        if srcLatestSnapshotLastLogIndex < response.snapshot.metadata.logEntryIndex =>
                      Future.failed(
                        SnapshotUpdateConflictException(
                          typeName,
                          srcMemberIndex,
                          entityId,
                          expectLogIndex = srcLatestSnapshotLastLogIndex,
                          actualLogIndex = response.snapshot.metadata.logEntryIndex,
                        ),
                      )
                    case response: SnapshotProtocol.SnapshotFound =>
                      Future.successful(response)
                    case response: SnapshotProtocol.SnapshotNotFound =>
                      Future.failed(SnapshotNotFoundException(typeName, srcMemberIndex, response.entityId))
                  }
              }
              saveSnapshotResult <- {
                val snapshot = fetchSnapshotResult.snapshot
                ask(dstShardSnapshotStore, replyTo => SnapshotProtocol.SaveSnapshot(snapshot, replyTo))
                  .mapTo[SnapshotProtocol.SaveSnapshotResponse]
                  .flatMap {
                    case response: SnapshotProtocol.SaveSnapshotSuccess =>
                      Future.successful(response)
                    case response: SnapshotProtocol.SaveSnapshotFailure =>
                      Future.failed(SaveSnapshotFailureException(typeName, dstMemberIndex, response.metadata))
                  }
              }
            } yield saveSnapshotResult
          }
          .fold(envelope)((e, _) => e)
      }
      .toMat(Sink.lastOption)(Keep.both)
      .mapMaterializedValue {
        case (killSwitch, envelope) =>
          (
            killSwitch,
            envelope.map {
              case Some(e) =>
                SyncCompleteAll(e.event.snapshotLastLogTerm, e.event.snapshotLastLogIndex, e.offset)
              case None =>
                SyncIncomplete()
            },
          )
      }
      .run()
  }
}
