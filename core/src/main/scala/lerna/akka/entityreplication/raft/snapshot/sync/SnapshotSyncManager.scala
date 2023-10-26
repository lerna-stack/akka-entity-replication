package lerna.akka.entityreplication.raft.snapshot.sync

import akka.actor.{ ActorRef, Props, Status }
import akka.event.{ Logging, LoggingAdapter }
import akka.pattern.extended.ask
import akka.pattern.pipe
import akka.persistence.{
  PersistentActor,
  RecoveryCompleted,
  RuntimePluginConfig,
  SnapshotOffer,
  SnapshotSelectionCriteria,
}
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.persistence.query.scaladsl.CurrentEventsByTagQuery
import akka.stream.{ KillSwitches, UniqueKillSwitch }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
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

import scala.concurrent.{ ExecutionContext, Future }

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

  final case class EntitySnapshotsUpdated(
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
      eventType: Class[_],
      persistenceId: String,
      sequenceNr: Long,
      offset: Offset,
  ) {

    def mergeEntityIds(other: EntitySnapshotsUpdated): EntitySnapshotsUpdated =
      copy(entityIds = entityIds ++ other.entityIds)

    /**
      * This format is used by logging
      */
    override def toString: String = {
      // omitted entityIds for simplicity
      val args = Seq(
        s"snapshotLastLogTerm: ${snapshotLastLogTerm.term}",
        s"snapshotLastLogIndex: ${snapshotLastLogIndex.underlying}",
        s"eventType: ${eventType.getName}",
        s"persistenceId: ${persistenceId}",
        s"sequenceNr: ${sequenceNr}",
        s"offset: ${offset}",
      )
      s"${getClass.getSimpleName}(${args.mkString(", ")})"
    }
  }

  sealed trait SyncStatus
  final case class SyncCompletePartially(
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
      offset: Offset,
  ) extends SyncStatus {

    def addEntityId(entityId: NormalizedEntityId): SyncCompletePartially =
      copy(entityIds = entityIds + entityId)
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

/**
  * Executes snapshot synchronization
  *
  * `SnapshotSyncManager` executes only one snapshot synchronization in its lifecycle.
  * This design is related to the following aspects:
  *
  *  - It reads snapshots from a [[lerna.akka.entityreplication.raft.snapshot.SnapshotStore]] that it spawns as its
  *    descendant. The `SnapshotStore` is a different instance from a `SnapshotStore` running as a descendant of
  *    [[lerna.akka.entityreplication.raft.RaftActor]] of the source replica group. The `SnapshotStore` cannot read new
  *    snapshots the other one wrote (by such as compaction) if it has already recovered from persisted data.
  *  - If it reuses an instance of `SnapshotStore` among multiple snapshot synchronizations, it cannot read a new
  *    snapshot written by a compaction that happens during the synchronization. It should spawn dedicated instances of
  *    `SnapshotStore` each synchronization or execute only one snapshot synchronization in its lifecycle.
  */
private[entityreplication] class SnapshotSyncManager(
    typeName: TypeName,
    srcMemberIndex: MemberIndex,
    dstMemberIndex: MemberIndex,
    dstShardSnapshotStore: ActorRef,
    shardId: NormalizedShardId,
    settings: RaftSettings,
) extends PersistentActor
    with RuntimePluginConfig {
  import SnapshotSyncManager._

  /**
    * NOTE:
    * [[SnapshotSyncManager]] has to use the same journal plugin as RaftActor
    * because snapshot synchronization is achieved by reading both the events
    * [[CompactionCompleted]] which RaftActor persisted and SnapshotCopied which [[SnapshotSyncManager]] persisted.
    */
  override def journalPluginId: String = settings.journalPluginId

  override def journalPluginConfig: Config = settings.journalPluginAdditionalConfig

  override def snapshotPluginId: String = settings.snapshotStorePluginId

  override def snapshotPluginConfig: Config = ConfigFactory.empty()

  override def persistenceId: String =
    SnapshotSyncManager.persistenceId(
      typeName,
      srcMemberIndex = srcMemberIndex,
      dstMemberIndex = dstMemberIndex,
      shardId,
    )

  private implicit val log: LoggingAdapter =
    Logging(this.context.system, this)

  private val shouldDeleteOldEvents: Boolean =
    settings.snapshotSyncDeleteOldEvents
  private val shouldDeleteOldSnapshots: Boolean =
    settings.snapshotSyncDeleteOldSnapshots
  private val deletionRelativeSequenceNr: Long =
    settings.snapshotSyncDeleteBeforeRelativeSequenceNr

  private[this] val readJournal =
    PersistenceQuery(context.system)
      .readJournalFor[CurrentEventsByTagQuery](settings.queryPluginId)

  private[this] val sourceShardSnapshotStore =
    context.actorOf(ShardSnapshotStore.props(typeName, settings, srcMemberIndex))

  override def receiveRecover: Receive = {

    case SnapshotOffer(metadata, snapshot: SyncProgress) =>
      if (log.isInfoEnabled) {
        log.info("Loaded snapshot: metadata=[{}], snapshot=[{}]", metadata, snapshot)
      }
      this.state = snapshot

    case event: Event =>
      event match {
        case _: SnapshotCopied =>
          updateState(event)
        case _: SyncCompleted =>
          updateState(event)
          context.become(ready)
      }

    case RecoveryCompleted =>
      if (log.isInfoEnabled) {
        log.info("Recovery completed: state=[{}]", this.state)
      }
  }

  private[this] var state = SyncProgress(Offset.noOffset)

  private[this] var killSwitch: Option[UniqueKillSwitch] = None

  override def receiveCommand: Receive = ready

  /** Behavior in the ready state
    *
    * `SnapshotSyncManager` accepts a new snapshot synchronization request. After it accepts the request, it will
    * transit to the synchronizing state for executing the synchronization.
    */
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
      stopSelf()

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

    case syncStatus: SyncStatus =>
      if (log.isWarningEnabled) {
        log.warning("Dropping unexpected SyncStatus: [{}]", syncStatus)
      }

    case failureStatus: Status.Failure =>
      if (log.isWarningEnabled) {
        log.warning("Dropping unexpected Status.Failure: [{}]", failureStatus)
      }

    case akkaPersistenceMessage if handleAkkaPersistenceMessage.isDefinedAt(akkaPersistenceMessage) =>
      if (log.isWarningEnabled) {
        log.warning("Dropping unexpected Akka Persistence message: [{}]", akkaPersistenceMessage)
      }

  }

  /** Behavior in the synchronizing state
    *
    * `SnapshotSyncManager` synchronizes entity snapshots in the background. It will send the synchronization result to
    * the reply-to actor after it completes the synchronization. It also will transit to the finalizing state if any
    * finalizing (deleting events or snapshots) is needed.
    */
  def synchronizing(
      replyTo: ActorRef,
      srcLatestSnapshotLastLogIndex: LogEntryIndex,
      dstLatestSnapshotLastLogTerm: Term,
      dstLatestSnapshotLastLogIndex: LogEntryIndex,
  ): Receive = {

    case syncSnapshot: SyncSnapshot =>
      if (log.isDebugEnabled) {
        log.debug("Dropping [{}] since the snapshot synchronization is running.", syncSnapshot)
      }

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
              if (log.isDebugEnabled) {
                log.debug(
                  "Snapshot synchronization partially completed and continues: {} -> {}",
                  s"(typeName: $typeName, memberIndex: $srcMemberIndex, snapshotLastLogIndex: ${event.snapshotLastLogIndex}/${srcLatestSnapshotLastLogIndex})",
                  s"(typeName: $typeName, memberIndex: $dstMemberIndex, snapshotLastLogTerm: ${dstLatestSnapshotLastLogTerm.term}, snapshotLastLogIndex: $dstLatestSnapshotLastLogIndex)",
                )
              }
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
                context.become(finalizing)
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
          stopSelf()
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
      stopSelf()

    case akkaPersistenceMessage if handleAkkaPersistenceMessage.isDefinedAt(akkaPersistenceMessage) =>
      if (log.isWarningEnabled) {
        log.warning("Dropping unexpected Akka Persistence message: [{}]", akkaPersistenceMessage)
      }

  }

  /** Behavior in the finalizing state
    *
    * `SnapshotSyncManager` handles finalizing (saving snapshots, deleting events, and deleting snapshots) and stops
    * eventually.
    */
  private def finalizing: Receive = {

    case syncSnapshot: SyncSnapshot =>
      if (log.isDebugEnabled) {
        log.debug("Dropping [{}] since the snapshot synchronization is finalizing.", syncSnapshot)
      }

    case syncStatus: SyncStatus =>
      if (log.isWarningEnabled) {
        log.warning("Dropping unexpected SyncStatus: [{}]", syncStatus)
      }

    case failureStatus: Status.Failure =>
      if (log.isWarningEnabled) {
        log.warning("Dropping unexpected Status.Failure: [{}]", failureStatus)
      }

    case akkaPersistenceMessage if handleAkkaPersistenceMessage.isDefinedAt(akkaPersistenceMessage) =>
      // This actor will stop eventually after all deletions are completed.
      handleAkkaPersistenceMessage(akkaPersistenceMessage)

  }

  def updateState(event: Event): Unit =
    event match {
      case event: SnapshotCopied =>
        this.state = SyncProgress(event.offset)
      case SyncCompleted(offset) =>
        this.state = SyncProgress(offset)
    }

  private def stopSelf(): Unit = {
    if (log.isInfoEnabled) {
      log.info("Stopping itself.")
    }
    context.stop(self)
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
    implicit val timeout: Timeout = Timeout(settings.snapshotSyncPersistenceOperationTimeout)

    readJournal
      .currentEventsByTag(EntitySnapshotsUpdatedTag(srcMemberIndex, shardId).toString, offset)
      .viaMat(KillSwitches.single)(Keep.right)
      .collect {
        case EventEnvelope(offset, persistenceId, sequenceNr, event: CompactionCompleted) =>
          EntitySnapshotsUpdated(
            event.snapshotLastLogTerm,
            event.snapshotLastLogIndex,
            event.entityIds,
            eventType = event.getClass,
            persistenceId = persistenceId,
            sequenceNr = sequenceNr,
            offset,
          )
        case EventEnvelope(offset, persistenceId, sequenceNr, event: SnapshotCopied) =>
          EntitySnapshotsUpdated(
            event.snapshotLastLogTerm,
            event.snapshotLastLogIndex,
            event.entityIds,
            eventType = event.getClass,
            persistenceId = persistenceId,
            sequenceNr = sequenceNr,
            offset,
          )
      }
      .log("entity-snapshots-updated-events")
      .filter { event =>
        dstLatestSnapshotLastLogTerm <= event.snapshotLastLogTerm &&
        dstLatestSnapshotLastLogIndex < event.snapshotLastLogIndex
      }
      .scan(Option.empty[EntitySnapshotsUpdated]) {
        // verify events ordering
        //
        // It is important to do this before `takeWhile`
        // because an extra element that `takeWhile` discards and the next batch will process
        // allows us verify all events without omissions.
        case (None, current) =>
          Option(current) // No comparisons
        case (Some(prev), current) =>
          if (
            prev.snapshotLastLogTerm <= current.snapshotLastLogTerm &&
            prev.snapshotLastLogIndex <= current.snapshotLastLogIndex
          ) {
            // correct order
            Option(current)
          } else {
            val ex =
              new IllegalStateException(s"The current EntitySnapshotsUpdated event is older than the previous one")
            if (log.isErrorEnabled)
              log.error(
                ex,
                "It must process events in ascending order of snapshotLastLogTerm and snapshotLastLogIndex [prev: {}, current: {}]",
                prev,
                current,
              )
            throw ex
          }
      }
      .mapConcat(identity) // flatten the `Option` element
      .statefulMapConcat { () =>
        var numberOfElements, numberOfEntities = 0;
        { event =>
          numberOfElements += 1
          numberOfEntities += event.entityIds.size
          (numberOfElements, event, numberOfEntities) :: Nil
        }
      }
      .takeWhile {
        case (numberOfElements, _, numberOfEntities) =>
          // take at least one element
          numberOfElements <= 1 || numberOfEntities <= settings.snapshotSyncMaxSnapshotBatchSize
      }
      .map { case (_, event, _) => event }
      .fold(Option.empty[EntitySnapshotsUpdated]) {
        // merge into single EntitySnapshotsUpdated
        case (None, newEvent)        => Option(newEvent)
        case (Some(event), newEvent) =>
          // prefer the latest term, index and offset
          Option(newEvent.mergeEntityIds(event))
      }
      .mapConcat(identity) // flatten the `Option` element
      .flatMapConcat { event =>
        Source(event.entityIds)
          .mapAsync(settings.snapshotSyncCopyingParallelism) { entityId =>
            copyEntitySnapshot(entityId, srcLatestSnapshotLastLogIndex)
          }
          .fold(
            SyncCompletePartially(
              event.snapshotLastLogTerm,
              event.snapshotLastLogIndex,
              entityIds = Set.empty,
              event.offset,
            ),
          ) { (status, destinationEntitySnapshotMetadata) =>
            status.addEntityId(destinationEntitySnapshotMetadata.entityId)
          }
      }
      .orElse(Source.single(SyncIncomplete()))
      .toMat(Sink.last)(Keep.both)
      .run()
  }

  private def copyEntitySnapshot(
      entityId: NormalizedEntityId,
      srcLatestSnapshotLastLogIndex: LogEntryIndex,
  )(implicit entitySnapshotOperationTimeout: Timeout): Future[SnapshotProtocol.EntitySnapshotMetadata] = {
    implicit val executionContext: ExecutionContext = context.dispatcher
    if (log.isDebugEnabled) {
      log.debug(
        "Copying EntitySnapshot: typeName=[{}], shardId=[{}], entityId=[{}], " +
        s"from=[$sourceShardSnapshotStore], to=[$dstShardSnapshotStore]",
        typeName,
        shardId.raw,
        entityId.raw,
      )
    }
    for {
      srcEntitySnapshot <- {
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
              Future.successful(response.snapshot)
            case response: SnapshotProtocol.SnapshotNotFound =>
              Future.failed(SnapshotNotFoundException(typeName, srcMemberIndex, response.entityId))
          }
      }
      dstEntitySnapshotMetadata <- {
        ask(dstShardSnapshotStore, replyTo => SnapshotProtocol.SaveSnapshot(srcEntitySnapshot, replyTo))
          .mapTo[SnapshotProtocol.SaveSnapshotResponse]
          .flatMap {
            case response: SnapshotProtocol.SaveSnapshotSuccess =>
              Future.successful(response.metadata)
            case response: SnapshotProtocol.SaveSnapshotFailure =>
              Future.failed(SaveSnapshotFailureException(typeName, dstMemberIndex, response.metadata))
          }
      }
    } yield {
      if (log.isDebugEnabled) {
        log.debug(
          "Copied EntitySnapshot: typeName=[{}], shardId=[{}], entityId=[{}], " +
          s"from=[$sourceShardSnapshotStore], to=[$dstShardSnapshotStore], " +
          s"sourceEntitySnapshotMetadata=[${srcEntitySnapshot.metadata}], " +
          s"destinationEntitySnapshotMetadata=[${dstEntitySnapshotMetadata}]",
          typeName,
          shardId.raw,
          entityId.raw,
        )
      }
      dstEntitySnapshotMetadata
    }
  }

  /** Handles an Akka Persistence message
    *
    * `SnapshotSyncManager` will stop after it handles a series of snapshot save, (optional) event deletion, and
    * (optional) snapshot deletion.
    *
    * `SnapshotSyncManager` will delete events first, then snapshot. It's because:
    *
    * If `SnapshotSyncManager` deletes events and snapshots in parallel order, there is a subtle timing at which the
    * snapshot deletion completes before the event deletion completes, and vice-versa. Suppose `SnapshotSyncManager`
    * stops immediately after the snapshot deletion completes, but the event deletion is not yet completed. In that
    * case, `SnapshotSyncManager` cannot log a messages of the event deletion completion. For this reason,
    * `SnapshotSyncManager` should delete messages and snapshots in serial order or stop after waiting for both deletion
    * completions.
    *
    * While `SnapshotSyncManager` can choose the order of such deletions, deleting messages before snapshots enables
    * `SnapshotSyncManager` to stop as soon as possible. Note that an upper sequence number of event deletion is less
    * than or equal to one of snapshot deletion. If `SnapshotSyncManager` deletes no events, it also deletes no
    * snapshots. In that case, `SnapshotSyncManager` can immediately stop after it deletes no events. Conversely, there
    * is a chance to delete events even if the manager deletes no snapshots. In this case, `SnapshotSyncManager` cannot
    * immediately stop.
    */
  private def handleAkkaPersistenceMessage: Receive = {
    case success: akka.persistence.SaveSnapshotSuccess =>
      handleSaveSnapshotSuccess(success)
    case failure: akka.persistence.SaveSnapshotFailure =>
      handleSaveSnapshotFailure(failure)
    case success: akka.persistence.DeleteMessagesSuccess =>
      handleDeleteMessagesSuccess(success)
    case failure: akka.persistence.DeleteMessagesFailure =>
      handleDeleteMessagesFailure(failure)
    case success: akka.persistence.DeleteSnapshotsSuccess =>
      handleDeleteSnapshotsSuccess(success)
    case failure: akka.persistence.DeleteSnapshotsFailure =>
      handleDeleteSnapshotsFailure(failure)
  }

  private def handleSaveSnapshotSuccess(success: akka.persistence.SaveSnapshotSuccess): Unit = {
    if (log.isInfoEnabled) {
      log.info("Succeeded to saveSnapshot given metadata [{}]", success.metadata)
    }
    if (shouldDeleteOldEvents) {
      deleteOldEvents(success.metadata.sequenceNr - deletionRelativeSequenceNr)
      // This actor will delete old snapshots after the event deletion succeeds.
    } else if (shouldDeleteOldSnapshots) {
      deleteOldSnapshots(success.metadata.sequenceNr - deletionRelativeSequenceNr)
    } else {
      // Stop itself if no deletions are enabled.
      // If any deletion is enabled, this actor will stop after the deletion completes (succeeds or fails).
      stopSelf()
    }
  }

  private def handleSaveSnapshotFailure(failure: akka.persistence.SaveSnapshotFailure): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Failed to saveSnapshot given metadata [{}] due to: [{}: {}]",
        failure.metadata,
        failure.cause.getClass.getCanonicalName,
        failure.cause.getMessage,
      )
    }
    stopSelf()
  }

  private def handleDeleteMessagesSuccess(success: akka.persistence.DeleteMessagesSuccess): Unit = {
    if (log.isInfoEnabled) {
      log.info("Succeeded to deleteMessages up to sequenceNr [{}]", success.toSequenceNr)
    }
    if (shouldDeleteOldSnapshots) {
      // Subtraction of `deletionRelativeSequenceNr` is not needed, since it's already subtracted at the event deletion.
      deleteOldSnapshots(success.toSequenceNr)
      // This actor will stop after the snapshot deletion completes (succeeds or fails).
    } else {
      stopSelf()
    }
  }

  private def handleDeleteMessagesFailure(failure: akka.persistence.DeleteMessagesFailure): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Failed to deleteMessages given toSequenceNr [{}] due to: [{}: {}]",
        failure.toSequenceNr,
        failure.cause.getClass.getCanonicalName,
        failure.cause.getMessage,
      )
    }
    stopSelf()
  }

  private def handleDeleteSnapshotsSuccess(success: akka.persistence.DeleteSnapshotsSuccess): Unit = {
    if (log.isInfoEnabled) {
      log.info("Succeeded to deleteSnapshots given criteria [{}]", success.criteria)
    }
    stopSelf()
  }

  private def handleDeleteSnapshotsFailure(failure: akka.persistence.DeleteSnapshotsFailure): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Failed to deleteSnapshots given criteria [{}] due to: [{}: {}]",
        failure.criteria,
        failure.cause.getClass.getCanonicalName,
        failure.cause.getMessage,
      )
    }
    stopSelf()
  }

  private def deleteOldEvents(upperSequenceNr: Long): Unit = {
    assert(shouldDeleteOldEvents, s"Old event deletion should be enabled but is disabled.")
    val deleteEventsToSequenceNr = math.max(0, upperSequenceNr)
    if (deleteEventsToSequenceNr > 0) {
      if (log.isDebugEnabled) {
        log.debug("Deleting events up to sequenceNr [{}]", deleteEventsToSequenceNr)
      }
      deleteMessages(deleteEventsToSequenceNr)
    } else {
      // Stop itself if it deletes no events.
      // No event deletion means that it will also delete no snapshots.
      stopSelf()
    }
  }

  private def deleteOldSnapshots(upperSequenceNr: Long): Unit = {
    assert(shouldDeleteOldSnapshots, "Old snapshot deletion should be enabled but is disabled.")
    // Since this actor will use the snapshot with sequence number `upperSequenceNr` for recovery,
    // it can delete snapshots with sequence numbers less than `upperSequenceNr`.
    val deleteSnapshotsToSequenceNr = math.max(0, upperSequenceNr - 1)
    if (deleteSnapshotsToSequenceNr > 0) {
      val deletionCriteria =
        SnapshotSelectionCriteria(minSequenceNr = 0, maxSequenceNr = deleteSnapshotsToSequenceNr)
      if (log.isDebugEnabled) {
        log.debug("Deleting snapshots matching criteria [{}]", deletionCriteria)
      }
      deleteSnapshots(deletionCriteria)
    } else {
      // Stop itself if it deletes no snapshots.
      stopSelf()
    }
  }

}
