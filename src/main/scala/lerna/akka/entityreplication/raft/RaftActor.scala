package lerna.akka.entityreplication.raft

import akka.actor.{ ActorRef, Cancellable, Props, Stash }
import akka.persistence.RuntimePluginConfig
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplication.EntityPropsProvider
import lerna.akka.entityreplication.ReplicationRegion.Msg
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftProtocol.{ Replicate, _ }
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStore
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager
import lerna.akka.entityreplication.util.ActorIds
import lerna.akka.entityreplication.{ ClusterReplicationSerializable, ReplicationActorContext, ReplicationRegion }

private[entityreplication] object RaftActor {

  def props(
      typeName: TypeName,
      extractEntityId: PartialFunction[Msg, (NormalizedEntityId, Msg)],
      replicationActorProps: EntityPropsProvider,
      region: ActorRef,
      shardSnapshotStoreProps: Props,
      selfMemberIndex: MemberIndex,
      otherMemberIndexes: Set[MemberIndex],
      settings: RaftSettings,
      maybeCommitLogStore: Option[CommitLogStore],
  ) =
    Props(
      new RaftActor(
        typeName,
        extractEntityId,
        replicationActorProps,
        region,
        shardSnapshotStoreProps,
        selfMemberIndex,
        otherMemberIndexes,
        settings,
        maybeCommitLogStore,
      ),
    )

  sealed trait State
  final case object Recovering extends State
  final case object Follower   extends State
  final case object Candidate  extends State
  final case object Leader     extends State

  sealed trait TimerEvent
  case object ElectionTimeout  extends TimerEvent
  case object HeartbeatTimeout extends TimerEvent
  case object SnapshotTick     extends TimerEvent

  sealed trait DomainEvent

  sealed trait PersistEvent                                  extends DomainEvent
  final case class BegunNewTerm(term: Term)                  extends PersistEvent with ClusterReplicationSerializable
  final case class Voted(term: Term, candidate: MemberIndex) extends PersistEvent with ClusterReplicationSerializable
  final case class DetectedNewTerm(term: Term)               extends PersistEvent with ClusterReplicationSerializable
  final case class AppendedEntries(term: Term, logEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex)
      extends PersistEvent
      with ClusterReplicationSerializable
  final case class AppendedEvent(event: EntityEvent) extends PersistEvent with ClusterReplicationSerializable
  final case class CompactionCompleted(
      memberIndex: MemberIndex,
      shardId: NormalizedShardId,
      snapshotLastLogTerm: Term,
      snapshotLastLogIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
  ) extends PersistEvent
      with ClusterReplicationSerializable
  final case class SnapshotSyncStarted(snapshotLastLogTerm: Term, snapshotLastLogIndex: LogEntryIndex)
      extends PersistEvent
      with ClusterReplicationSerializable
  final case class SnapshotSyncCompleted(snapshotLastLogTerm: Term, snapshotLastLogIndex: LogEntryIndex)
      extends PersistEvent
      with ClusterReplicationSerializable

  sealed trait NonPersistEvent                                                                  extends DomainEvent
  final case class BecameFollower()                                                             extends NonPersistEvent
  final case class BecameCandidate()                                                            extends NonPersistEvent
  final case class BecameLeader()                                                               extends NonPersistEvent
  final case class DetectedLeaderMember(leaderMember: MemberIndex)                              extends NonPersistEvent
  final case class StartedReplication(client: ClientContext, logEntryIndex: LogEntryIndex)      extends NonPersistEvent
  final case class AcceptedRequestVote(follower: MemberIndex)                                   extends NonPersistEvent
  final case class SucceededAppendEntries(follower: MemberIndex, lastLogIndex: LogEntryIndex)   extends NonPersistEvent
  final case class DeniedAppendEntries(follower: MemberIndex)                                   extends NonPersistEvent
  final case class FollowedLeaderCommit(leaderMember: MemberIndex, leaderCommit: LogEntryIndex) extends NonPersistEvent
  final case class Committed(logEntryIndex: LogEntryIndex)                                      extends NonPersistEvent
  final case class SnapshottingStarted(term: Term, logEntryIndex: LogEntryIndex, entityIds: Set[NormalizedEntityId])
      extends NonPersistEvent
  final case class EntitySnapshotSaved(metadata: EntitySnapshotMetadata) extends NonPersistEvent
  final case class PassivatedEntity(entityId: NormalizedEntityId)        extends NonPersistEvent
  final case class TerminatedEntity(entityId: NormalizedEntityId)        extends NonPersistEvent

  trait NonPersistEventLike extends NonPersistEvent // テスト用
}

private[raft] class RaftActor(
    typeName: TypeName,
    val extractEntityId: PartialFunction[Msg, (NormalizedEntityId, Msg)],
    replicationActorProps: EntityPropsProvider,
    _region: ActorRef,
    shardSnapshotStoreProps: Props,
    _selfMemberIndex: MemberIndex,
    _otherMemberIndexes: Set[MemberIndex],
    val settings: RaftSettings,
    maybeCommitLogStore: Option[CommitLogStore],
) extends RaftActorBase
    with RuntimePluginConfig
    with Stash
    with Follower
    with Candidate
    with Leader {
  import RaftActor._
  import context.dispatcher

  protected[this] def shardId: NormalizedShardId = NormalizedShardId.from(self.path)

  protected[this] def region: ActorRef = _region

  private[this] val shardSnapshotStoreNamePrefix = "ShardSnapshotStore"

  private[this] val snapshotSyncManagerNamePrefix = "SnapshotSyncManager"

  private[this] val shardSnapshotStore: ActorRef =
    context.actorOf(shardSnapshotStoreProps, ActorIds.actorName(shardSnapshotStoreNamePrefix, shardId.underlying))

  protected[this] def selfMemberIndex: MemberIndex = _selfMemberIndex

  protected[this] def otherMemberIndexes: Set[MemberIndex] = _otherMemberIndexes

  protected[this] def createEntityIfNotExists(entityId: NormalizedEntityId): Unit = replicationActor(entityId)

  protected def receiveFetchEntityEvents(request: FetchEntityEvents): Unit = {
    val logEntries =
      currentData.selectEntityEntries(request.entityId, from = request.from, to = request.to)
    request.replyTo ! FetchEntityEventsResponse(logEntries)
  }

  protected[this] def replicationActor(entityId: NormalizedEntityId): ActorRef = {
    context.child(entityId.underlying).getOrElse {
      if (log.isDebugEnabled)
        log.debug(
          "=== [{}] created an entity ({}) ===",
          currentState,
          entityId,
        )
      val props  = replicationActorProps(new ReplicationActorContext(entityId.raw, self))
      val entity = context.watchWith(context.actorOf(props, entityId.underlying), EntityTerminated(entityId))
      entity ! Activate(shardSnapshotStore, recoveryIndex = currentData.lastApplied)
      entity
    }
  }

  override val persistenceId: String =
    ActorIds.persistenceId("raft", typeName.underlying, shardId.underlying, selfMemberIndex.role)

  override def journalPluginId: String = settings.journalPluginId

  override def journalPluginConfig: Config = settings.journalPluginAdditionalConfig

  override def snapshotPluginId: String = settings.snapshotStorePluginId

  override def snapshotPluginConfig: Config = ConfigFactory.empty()

  val numberOfMembers: Int = settings.replicationFactor

  protected def updateState(domainEvent: DomainEvent): RaftMemberData =
    domainEvent match {
      case BegunNewTerm(term) =>
        currentData.syncTerm(term)
      case Voted(term, candidate) =>
        currentData.vote(candidate, term)
      case DetectedNewTerm(term) =>
        currentData.syncTerm(term)
      case AppendedEntries(term, logEntries, prevLogIndex) =>
        currentData
          .syncTerm(term)
          .appendEntries(logEntries, prevLogIndex)
      case AppendedEvent(event) =>
        currentData
          .appendEvent(event)
      case BecameFollower() =>
        currentData.initializeFollowerData()
      case BecameCandidate() =>
        currentData.initializeCandidateData()
      case BecameLeader() =>
        if (log.isInfoEnabled)
          log.info(
            "[Leader] New leader was elected (term: {}, lastLogTerm: {}, lastLogIndex: {})",
            currentData.currentTerm,
            currentData.replicatedLog.lastLogTerm,
            currentData.replicatedLog.lastLogIndex,
          )
        currentData.initializeLeaderData()
      case DetectedLeaderMember(leaderMember) =>
        currentData.detectLeaderMember(leaderMember)
      case AcceptedRequestVote(follower) =>
        currentData.acceptedBy(follower)
      case StartedReplication(client, logEntryIndex) =>
        currentData.registerClient(client, logEntryIndex)
      case SucceededAppendEntries(follower, lastLogIndex) =>
        currentData.syncLastLogIndex(follower, lastLogIndex)
      case DeniedAppendEntries(follower) =>
        currentData.markSyncLogFailed(follower)
      case FollowedLeaderCommit(leaderMember, leaderCommit) =>
        currentData
          .detectLeaderMember(leaderMember)
          .followLeaderCommit(leaderCommit)
          .applyCommittedLogEntries { logEntries =>
            logEntries.foreach { logEntry =>
              applyToReplicationActor(logEntry)
              maybeCommitLogStore.foreach(_.save(shardId, logEntry.index, logEntry.event.event))
            }
          }
      case Committed(logEntryIndex) =>
        currentData
          .commit(logEntryIndex)
          .handleCommittedLogEntriesAndClients { entries =>
            maybeCommitLogStore.foreach(store => {
              entries.map(_._1).foreach(logEntry => store.save(shardId, logEntry.index, logEntry.event.event))
            })
            entries.foreach {
              case (logEntry, Some(client)) =>
                if (log.isDebugEnabled)
                  log.debug("=== [Leader] committed {} and will notify it to {} ===", logEntry, client)
                client.ref.tell(
                  ReplicationSucceeded(logEntry.event.event, logEntry.index, client.instanceId),
                  client.originSender.getOrElse(ActorRef.noSender),
                )
              case (logEntry, None) =>
                // 復旧中の commit or リーダー昇格時に未コミットのログがあった場合の commit
                applyToReplicationActor(logEntry)
            }
          }
      case SnapshottingStarted(term, logEntryIndex, entityIds) =>
        currentData.startSnapshotting(term, logEntryIndex, entityIds)
      case EntitySnapshotSaved(metadata) =>
        currentData.recordSavedSnapshot(metadata)
      case CompactionCompleted(_, _, snapshotLastTerm, snapshotLastIndex, _) =>
        currentData
          .updateLastSnapshotStatus(snapshotLastTerm, snapshotLastIndex)
          .compactReplicatedLog(settings.compactionPreserveLogSize)
      case SnapshotSyncStarted(snapshotLastLogTerm, snapshotLastLogIndex) =>
        currentData.startSnapshotSync(snapshotLastLogTerm, snapshotLastLogIndex)
      case SnapshotSyncCompleted(snapshotLastLogTerm, snapshotLastLogIndex) =>
        stopAllEntities()
        currentData.completeSnapshotSync(snapshotLastLogTerm, snapshotLastLogIndex)
      case PassivatedEntity(entityId) =>
        currentData.passivateEntity(entityId)
      case TerminatedEntity(entityId) =>
        currentData.terminateEntity(entityId)
      // TODO: Remove when test code is modified
      case _: NonPersistEventLike =>
        if (log.isErrorEnabled) log.error("must not use NonPersistEventLike in production code")
        currentData // ignore event
    }

  override protected val stateBehaviors: StateBehaviors = {
    case Recovering => recoveringBehavior
    case Follower   => followerBehavior
    case Candidate  => candidateBehavior
    case Leader     => leaderBehavior
  }

  override protected def onRecoveryCompleted(): Unit = {
    become(Follower)
    resetSnapshotTickTimer()
  }

  override protected val onTransition: TransitionHandler = {
    case _ -> Follower =>
      applyDomainEvent(BecameFollower()) { _ =>
        resetElectionTimeoutTimer()
        unstashAll()
      }
    case _ -> Candidate =>
      applyDomainEvent(BecameCandidate()) { _ =>
        resetElectionTimeoutTimer()
      }
    case Candidate -> Leader =>
      resetHeartbeatTimeoutTimer()
      applyDomainEvent(BecameLeader()) { _ =>
        self ! Replicate.internal(NoOp, self)
        unstashAll()
      }
  }

  def recoveringBehavior: Receive = {
    case _ => stash()
  }

  def receiveEntityTerminated(entityId: NormalizedEntityId): Unit = {
    if (currentData.entityStateOf(entityId).isPassivating) {
      applyDomainEvent(TerminatedEntity(entityId)) { _ => }
    } else {
      // restart
      replicationActor(entityId)
    }
  }

  def suspendEntity(entityId: NormalizedEntityId, stopMessage: Any): Unit = {
    if (log.isDebugEnabled) log.debug("=== [{}] suspend entity '{}' with {} ===", currentState, entityId, stopMessage)
    applyDomainEvent(PassivatedEntity(entityId)) { _ =>
      replicationActor(entityId) ! stopMessage
    }
  }

  def receiveEntitySnapshotResponse(response: Snapshot): Unit = {
    import SnapshotProtocol._
    val snapshot = EntitySnapshot(response.metadata, response.state)
    shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = self)
  }

  def receiveSaveSnapshotResponse(response: SnapshotProtocol.SaveSnapshotResponse): Unit =
    response match {
      case SnapshotProtocol.SaveSnapshotSuccess(metadata) =>
        applyDomainEvent(EntitySnapshotSaved(metadata)) { _ =>
          val progress = currentData.snapshottingProgress
          if (progress.isCompleted) {
            applyDomainEvent(
              CompactionCompleted(
                selfMemberIndex,
                shardId,
                progress.snapshotLastLogTerm,
                progress.snapshotLastLogIndex,
                progress.completedEntities,
              ),
            ) { _ =>
              saveSnapshot(currentData.persistentState) // Note that this persistence can fail
              if (log.isInfoEnabled)
                log.info(
                  "[{}] compaction completed (term: {}, logEntryIndex: {})",
                  currentState,
                  progress.snapshotLastLogTerm,
                  progress.snapshotLastLogIndex,
                )
            }
          }
        }
      case SnapshotProtocol.SaveSnapshotFailure(_) =>
      // do nothing
    }

  private[this] var electionTimeoutTimer: Option[Cancellable] = None

  def resetElectionTimeoutTimer(): Unit = {
    cancelElectionTimeoutTimer()
    val timeout = settings.randomizedElectionTimeout()
    if (log.isDebugEnabled) log.debug("=== [{}] election-timeout after {} ms ===", currentState, timeout.toMillis)
    electionTimeoutTimer = Some(context.system.scheduler.scheduleOnce(timeout, self, ElectionTimeout))
  }

  def cancelElectionTimeoutTimer(): Unit = {
    electionTimeoutTimer.foreach(_.cancel())
  }

  private[this] var heartbeatTimeoutTimer: Option[Cancellable] = None

  def resetHeartbeatTimeoutTimer(): Unit = {
    cancelHeartbeatTimeoutTimer()
    val timeout = settings.heartbeatInterval
    if (log.isDebugEnabled) log.debug("=== [Leader] Heartbeat after {} ms ===", settings.heartbeatInterval.toMillis)
    heartbeatTimeoutTimer = Some(context.system.scheduler.scheduleOnce(timeout, self, HeartbeatTimeout))
  }

  def cancelHeartbeatTimeoutTimer(): Unit = {
    heartbeatTimeoutTimer.foreach(_.cancel())
  }

  private[this] var snapshotTickTimer: Option[Cancellable] = None

  def resetSnapshotTickTimer(): Unit = {
    val timeout = settings.randomizedCompactionLogSizeCheckInterval()
    snapshotTickTimer.foreach(_.cancel())
    snapshotTickTimer = Some(context.system.scheduler.scheduleOnce(timeout, self, SnapshotTick))
  }

  def broadcast(message: Any): Unit = {
    if (log.isDebugEnabled) log.debug("=== [{}] broadcast {} ===", currentState, message)
    region ! ReplicationRegion.Broadcast(message)
  }

  def applyToReplicationActor(logEntry: LogEntry): Unit =
    logEntry.event match {
      case EntityEvent(_, NoOp) => // NoOp は replicationActor には関係ないので転送しない
      case EntityEvent(Some(entityId), event) =>
        if (log.isDebugEnabled) log.debug("=== [{}] applying {} to ReplicationActor ===", currentState, event)
        replicationActor(entityId) ! Replica(logEntry)
      case EntityEvent(None, event) =>
        if (log.isWarningEnabled)
          log.warning("=== [{}] {} was not applied, because it is not assigned any entity ===", currentState, event)
    }

  def handleSnapshotTick(): Unit = {
    if (
      currentData.replicatedLog.entries.size >= settings.compactionLogSizeThreshold
      && currentData.hasLogEntriesThatCanBeCompacted
    ) {
      val (term, logEntryIndex, entityIds) = currentData.resolveSnapshotTargets()
      applyDomainEvent(SnapshottingStarted(term, logEntryIndex, entityIds)) { _ =>
        if (log.isInfoEnabled)
          log.info(
            "[{}] compaction started (logEntryIndex: {}, number of entities: {})",
            currentState,
            logEntryIndex,
            entityIds.size,
          )
        requestTakeSnapshots(logEntryIndex, entityIds)
      }
    }
    resetSnapshotTickTimer()
  }

  def requestTakeSnapshots(logEntryIndex: LogEntryIndex, entityIds: Set[NormalizedEntityId]): Unit = {
    entityIds.foreach { entityId =>
      val metadata = EntitySnapshotMetadata(entityId, logEntryIndex)
      replicationActor(entityId) ! TakeSnapshot(metadata, self)
    }
  }

  protected def rejectAppendEntriesSinceSnapshotsAreDirty(appendEntries: AppendEntries): Unit = {
    require(
      currentData.lastSnapshotStatus.isDirty,
      "This method requires to be called when snapshot status is dirty",
    )
    // We have to wait for InstallSnapshot to update the all snapshots perfectly.
    if (currentData.hasMatchLogEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)) {
      // Ignore it for keeping leader's nextIndex
      if (log.isDebugEnabled)
        log.debug(
          "=== [{}] ignore {} because {} is still dirty ===",
          currentState,
          appendEntries,
          currentData.lastSnapshotStatus,
        )
      cancelElectionTimeoutTimer()
      if (appendEntries.term != currentData.currentTerm) {
        applyDomainEvent(DetectedNewTerm(appendEntries.term)) { _ =>
          applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
            become(Follower)
          }
        }
      } else {
        applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
          become(Follower)
        }
      }
    } else {
      // Reply AppendEntriesFailed command for decrementing leader's nextIndex
      if (log.isDebugEnabled)
        log.debug(
          "=== [{}] deny {} because the log (lastLogTerm: {}, lastLogIndex: {}) can not merge the entries ===",
          currentState,
          appendEntries,
          currentData.replicatedLog.lastLogTerm,
          currentData.replicatedLog.lastLogIndex,
        )
      cancelElectionTimeoutTimer()
      if (appendEntries.term != currentData.currentTerm) {
        applyDomainEvent(DetectedNewTerm(appendEntries.term)) { _ =>
          applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
            sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)
            become(Follower)
          }
        }
      } else {
        applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
          sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)
          become(Follower)
        }
      }
    }
  }

  protected def receiveInstallSnapshot(request: InstallSnapshot): Unit =
    /*
     * Take the following actions:
     * - lastSnapshotStatus.isDirty = true  && installSnapshot < lastSnapshotStatus => ignore
     * - lastSnapshotStatus.isDirty = true  && installSnapshot = lastSnapshotStatus => start snapshot-synchronization (for enabling retry)
     * - lastSnapshotStatus.isDirty = true  && installSnapshot > lastSnapshotStatus => start snapshot-synchronization (for enabling retry)
     * - lastSnapshotStatus.isDirty = false && installSnapshot < lastSnapshotStatus => ignore
     * - lastSnapshotStatus.isDirty = false && installSnapshot = lastSnapshotStatus => start snapshot-synchronization
     * - lastSnapshotStatus.isDirty = false && installSnapshot > lastSnapshotStatus => start snapshot-synchronization
     * Legend:
     *    - A < B: A is older than B
     *    - A = B: A is same as B
     *    - A > B: A is newer than B
     * NOTE:
     *   "start snapshot-synchronization" means delegating the process to SnapshotSyncManager.
     *   SnapshotSyncManager can ignore the command based on its own state.
     */
    request match {
      case installSnapshot if installSnapshot.term.isOlderThan(currentData.currentTerm) =>
      // ignore the message because this member knows another newer leader
      case installSnapshot
          if installSnapshot.srcLatestSnapshotLastLogTerm < currentData.lastSnapshotStatus.targetSnapshotLastTerm
          || installSnapshot.srcLatestSnapshotLastLogLogIndex < currentData.lastSnapshotStatus.targetSnapshotLastLogIndex =>
        // ignore the message because this member has already known newer snapshots and require overwriting with newer snapshots
        if (log.isDebugEnabled)
          log.debug(
            Seq(
              "=== [{}] ignore {} because this member may have already saved newer snapshots",
              "and requires overwriting them with newer snapshots",
              "(targetSnapshotLastTerm: {}, targetSnapshotLastLogIndex: {}) ===",
            ).mkString(" "),
            currentState,
            installSnapshot,
            currentData.lastSnapshotStatus.targetSnapshotLastTerm,
            currentData.lastSnapshotStatus.targetSnapshotLastLogIndex,
          )
      case installSnapshot =>
        if (installSnapshot.term == currentData.currentTerm) {
          applyDomainEvent(DetectedLeaderMember(installSnapshot.srcMemberIndex)) { _ =>
            applyDomainEvent(
              SnapshotSyncStarted(
                installSnapshot.srcLatestSnapshotLastLogTerm,
                installSnapshot.srcLatestSnapshotLastLogLogIndex,
              ),
            ) { _ =>
              startSyncSnapshot(installSnapshot)
              become(Follower)
            }
          }
        } else {
          applyDomainEvent(DetectedNewTerm(installSnapshot.term)) { _ =>
            applyDomainEvent(DetectedLeaderMember(installSnapshot.srcMemberIndex)) { _ =>
              applyDomainEvent(
                SnapshotSyncStarted(
                  installSnapshot.srcLatestSnapshotLastLogTerm,
                  installSnapshot.srcLatestSnapshotLastLogLogIndex,
                ),
              ) { _ =>
                startSyncSnapshot(installSnapshot)
                become(Follower)
              }
            }
          }
        }
    }

  protected def receiveSyncSnapshotResponse(response: SnapshotSyncManager.Response): Unit =
    response match {
      case response: SnapshotSyncManager.SyncSnapshotSucceeded =>
        applyDomainEvent(SnapshotSyncCompleted(response.snapshotLastLogTerm, response.snapshotLastLogIndex)) { _ =>
          region ! ReplicationRegion.DeliverTo(
            response.srcMemberIndex,
            InstallSnapshotSucceeded(
              shardId,
              currentData.currentTerm,
              currentData.replicatedLog.lastLogIndex,
              selfMemberIndex,
            ),
          )
        }

      case response: SnapshotSyncManager.SyncSnapshotAlreadySucceeded =>
        region ! ReplicationRegion.DeliverTo(
          response.srcMemberIndex,
          InstallSnapshotSucceeded(
            shardId,
            currentData.currentTerm,
            currentData.replicatedLog.lastLogIndex,
            selfMemberIndex,
          ),
        )

      case _: SnapshotSyncManager.SyncSnapshotFailed => // ignore
    }

  private val snapshotSyncManagerName: String = ActorIds.actorName(
    snapshotSyncManagerNamePrefix,
    typeName.underlying,
  )

  protected def startSyncSnapshot(installSnapshot: InstallSnapshot): Unit = {
    if (currentData.snapshottingProgress.isInProgress) {
      // Snapshot updates during compaction will break consistency
      if (log.isInfoEnabled)
        log.info(
          "Skipping snapshot synchronization because compaction is in progress (remaining: {}/{})",
          currentData.snapshottingProgress.inProgressEntities.size,
          currentData.snapshottingProgress.inProgressEntities.size + currentData.snapshottingProgress.completedEntities.size,
        )
    } else {
      val snapshotSyncManager =
        context.child(snapshotSyncManagerName).getOrElse {
          context.actorOf(
            SnapshotSyncManager.props(
              typeName = typeName,
              srcMemberIndex = installSnapshot.srcMemberIndex,
              dstMemberIndex = selfMemberIndex,
              dstShardSnapshotStore = shardSnapshotStore,
              shardId,
              settings,
            ),
            snapshotSyncManagerName,
          )
        }
      snapshotSyncManager ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = installSnapshot.srcLatestSnapshotLastLogTerm,
        srcLatestSnapshotLastLogIndex = installSnapshot.srcLatestSnapshotLastLogLogIndex,
        dstLatestSnapshotLastLogTerm = currentData.lastSnapshotStatus.snapshotLastTerm,
        dstLatestSnapshotLastLogIndex = currentData.lastSnapshotStatus.snapshotLastLogIndex,
        replyTo = self,
      )
    }
  }

  private[this] def stopAllEntities(): Unit = {
    // FIXME: Make it possible to stop only entities by using Actor hierarchy
    val excludes: Set[String] =
      Set(ActorIds.actorName(shardSnapshotStoreNamePrefix, ""), ActorIds.actorName(snapshotSyncManagerNamePrefix, ""))
    context.children.filterNot(c => excludes.exists(c.path.name.startsWith)).foreach { child =>
      context.stop(child)
    }
    if (log.isDebugEnabled)
      log.debug(
        "=== [{}] stopped all entities ===",
        currentState,
      )
  }

  override def postStop(): Unit = {
    cancelHeartbeatTimeoutTimer()
    cancelElectionTimeoutTimer()
    super.postStop()
  }
}
