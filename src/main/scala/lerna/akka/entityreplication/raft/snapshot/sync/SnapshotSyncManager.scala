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
import lerna.akka.entityreplication.raft.persistence.EntitySnapshotsUpdatedTag
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

  final case class SnapshotCopied(
      offset: Offset,
      memberIndex: MemberIndex,
      shardId: NormalizedShardId,
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
  ) extends Event
      with ClusterReplicationSerializable

  final case class SyncCompleted(offset: Offset) extends Event with ClusterReplicationSerializable

  sealed trait State

  final case class SyncProgress(offset: Offset) extends State with ClusterReplicationSerializable

  final case class CompactionEnvelope(
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
      offset: Offset,
  )

  sealed trait SyncStatus
  final case class SyncCompletePartially(
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
      offset: Offset,
  ) extends SyncStatus {

    def addEntityId(entityId: NormalizedEntityId): SyncCompletePartially =
      copy(entityIds = entityIds + entityId)

    def mergeEntityIds(other: SyncCompletePartially): SyncCompletePartially =
      copy(entityIds = entityIds ++ other.entityIds)
  }
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

  def persistenceId(
      typeName: TypeName,
      srcMemberIndex: MemberIndex,
      dstMemberIndex: MemberIndex,
      shardId: NormalizedShardId,
  ): String =
    ActorIds.persistenceId(
      "SnapshotSyncManager",
      typeName.underlying,
      srcMemberIndex.role,
      dstMemberIndex.role,
      shardId.underlying,
    )
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
    SnapshotSyncManager.persistenceId(
      typeName,
      srcMemberIndex = srcMemberIndex,
      dstMemberIndex = dstMemberIndex,
      shardId,
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
      if (log.isInfoEnabled)
        log.info(
          "Snapshot synchronization already completed: {} -> {}",
          s"(typeName: $typeName, memberIndex: $srcMemberIndex, snapshotLastLogTerm: ${srcLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $srcLatestSnapshotLastLogIndex)",
          s"(typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
        )
      context.stop(self)

    case SyncSnapshot(
          srcLatestSnapshotLastLogTerm,
          srcLatestSnapshotLastLogIndex,
          dstLatestSnapshotLastLogTerm,
          dstLatestSnapshotLastLogIndex,
          replyTo,
        ) =>
      startSnapshotSynchronizationBatch(
        srcLatestSnapshotLastLogIndex,
        dstLatestSnapshotLastLogTerm,
        dstLatestSnapshotLastLogIndex,
        state.offset,
      )
      context.become(
        synchronizing(
          replyTo,
          srcLatestSnapshotLastLogIndex = srcLatestSnapshotLastLogIndex,
          dstLatestSnapshotLastLogTerm = dstLatestSnapshotLastLogTerm,
          dstLatestSnapshotLastLogIndex = dstLatestSnapshotLastLogIndex,
        ),
      )
      if (log.isInfoEnabled)
        log.info(
          "Snapshot synchronization started: {} -> {}",
          s"(typeName: $typeName, memberIndex: $srcMemberIndex, snapshotLastLogTerm: ${srcLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $srcLatestSnapshotLastLogIndex)",
          s"(typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
        )

    case _: akka.persistence.SaveSnapshotSuccess =>
      context.stop(self)

    case _: akka.persistence.SaveSnapshotFailure =>
      context.stop(self)
  }

  def synchronizing(
      replyTo: ActorRef,
      srcLatestSnapshotLastLogIndex: LogEntryIndex,
      dstLatestSnapshotLastLogTerm: Term,
      dstLatestSnapshotLastLogIndex: LogEntryIndex,
  ): Receive = {

    case _: SyncSnapshot => // ignore

    case syncStatus: SyncStatus =>
      this.killSwitch = None
      syncStatus match {
        case completePartially: SyncCompletePartially =>
          val snapshotCopied = SnapshotCopied(
            completePartially.offset,
            dstMemberIndex,
            shardId,
            completePartially.snapshotLastLogTerm,
            completePartially.snapshotLastLogIndex,
            completePartially.entityIds,
          )
          persist(snapshotCopied) { event =>
            updateState(event)
            if (event.snapshotLastLogIndex < srcLatestSnapshotLastLogIndex) {
              // complete partially
              startSnapshotSynchronizationBatch(
                srcLatestSnapshotLastLogIndex,
                dstLatestSnapshotLastLogTerm,
                dstLatestSnapshotLastLogIndex,
                event.offset,
              )
            } else if (event.snapshotLastLogIndex == srcLatestSnapshotLastLogIndex) {
              // complete all
              persist(SyncCompleted(event.offset)) { event =>
                updateState(event)
                saveSnapshot(this.state)
                replyTo ! SyncSnapshotSucceeded(
                  completePartially.snapshotLastLogTerm,
                  completePartially.snapshotLastLogIndex,
                  srcMemberIndex,
                )
                if (log.isInfoEnabled)
                  log.info(
                    "Snapshot synchronization completed: {} -> {}",
                    s"(typeName: $typeName, memberIndex: $srcMemberIndex)",
                    s"(typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
                  )
              }
            } else {
              // illegal result: event.snapshotLastLogIndex > srcLatestSnapshotLastLogIndex
              //
              self ! Status.Failure(
                new IllegalStateException(
                  s"Found a snapshotLastLogIndex[${event.snapshotLastLogIndex}] that exceeds the srcLatestSnapshotLastLogIndex[${srcLatestSnapshotLastLogIndex}]",
                ),
              )
            }
          }
        case _: SyncIncomplete =>
          replyTo ! SyncSnapshotFailed()
          if (log.isInfoEnabled)
            log.info(
              "Snapshot synchronization is incomplete: {} -> {}",
              s"(typeName: $typeName, memberIndex: $srcMemberIndex)",
              s"(typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
            )
          context.stop(self)
      }

    case Status.Failure(e) =>
      this.killSwitch = None
      replyTo ! SyncSnapshotFailed()
      if (log.isWarningEnabled)
        log.warning(
          "Snapshot synchronization aborted: {} -> {} cause: {}",
          s"(typeName: $typeName, memberIndex: $srcMemberIndex)",
          s"(typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
          e,
        )
      context.stop(self)

    case _: akka.persistence.SaveSnapshotSuccess => // ignore: previous execution result
    case _: akka.persistence.SaveSnapshotFailure => // ignore: previous execution result
  }

  def updateState(event: Event): Unit =
    event match {
      case event: SnapshotCopied =>
        this.state = SyncProgress(event.offset)
      // keep current behavior
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

  private def startSnapshotSynchronizationBatch(
      srcLatestSnapshotLastLogIndex: LogEntryIndex,
      dstLatestSnapshotLastLogTerm: Term,
      dstLatestSnapshotLastLogIndex: LogEntryIndex,
      offset: Offset,
  ): Unit = {
    import context.dispatcher
    val (killSwitch, result) = synchronizeSnapshots(
      srcLatestSnapshotLastLogIndex,
      dstLatestSnapshotLastLogTerm,
      dstLatestSnapshotLastLogIndex,
      offset,
    )
    this.killSwitch = Option(killSwitch)
    result pipeTo self
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
      .currentEventsByTag(EntitySnapshotsUpdatedTag(srcMemberIndex, shardId).toString, offset)
      .viaMat(KillSwitches.single)(Keep.right)
      .collect {
        case EventEnvelope(offset, _, _, event: CompactionCompleted)
            if dstLatestSnapshotLastLogTerm <= event.snapshotLastLogTerm && dstLatestSnapshotLastLogIndex < event.snapshotLastLogIndex =>
          CompactionEnvelope(event.snapshotLastLogTerm, event.snapshotLastLogIndex, event.entityIds, offset)
        case EventEnvelope(offset, _, _, event: SnapshotCopied)
            if dstLatestSnapshotLastLogTerm <= event.snapshotLastLogTerm && dstLatestSnapshotLastLogIndex < event.snapshotLastLogIndex =>
          CompactionEnvelope(event.snapshotLastLogTerm, event.snapshotLastLogIndex, event.entityIds, offset)
      }
      .statefulMapConcat { () =>
        var numberOfElements, numberOfEntities = 0;
        { envelope =>
          numberOfElements += 1
          numberOfEntities += envelope.entityIds.size
          (numberOfElements, envelope, numberOfEntities) :: Nil
        }
      }
      .takeWhile {
        case (numberOfElements, _, numberOfEntities) =>
          // take at least one element
          numberOfElements <= 1 || numberOfEntities <= settings.snapshotSyncMaxSnapshotBatchSize
      }
      .flatMapConcat {
        case (_, envelope, _) =>
          Source(envelope.entityIds)
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
            .fold(
              SyncCompletePartially(
                envelope.snapshotLastLogTerm,
                envelope.snapshotLastLogIndex,
                entityIds = Set.empty,
                envelope.offset,
              ),
            )((status, e) => status.addEntityId(e.metadata.entityId))
      }
      .toMat(Sink.fold[SyncStatus, SyncCompletePartially](SyncIncomplete()) {
        case (_: SyncIncomplete, newResult)             => newResult
        case (status: SyncCompletePartially, newResult) =>
          // prefer the latest term, index and offset
          newResult.mergeEntityIds(status)
      })(Keep.both)
      .run()
  }
}
