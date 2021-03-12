package lerna.akka.entityreplication.raft

import akka.actor.{ ActorRef, Cancellable, Props, Stash }
import akka.persistence.RuntimePluginConfig
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ReplicationActor.Snapshot
import lerna.akka.entityreplication.ReplicationRegion.Msg
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftProtocol.{ Replicate, _ }
import lerna.akka.entityreplication.raft.eventhandler.CommitLogStore
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands.{ InstallSnapshot, InstallSnapshotSucceeded }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager
import lerna.akka.entityreplication.util.ActorIds
import lerna.akka.entityreplication.{ ClusterReplicationSerializable, ReplicationActor, ReplicationRegion }

object RaftActor {

  def props(
      typeName: String,
      extractEntityId: PartialFunction[Msg, (NormalizedEntityId, Msg)],
      replicationActorProps: Props,
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

  trait NonPersistEventLike extends NonPersistEvent // テスト用
}

class RaftActor(
    val typeName: String,
    val extractEntityId: PartialFunction[Msg, (NormalizedEntityId, Msg)],
    replicationActorProps: Props,
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

  protected[akka] def recoveryEntity(entityId: NormalizedEntityId): Unit = {
    shardSnapshotStore ! SnapshotProtocol.FetchSnapshot(entityId, replyTo = self)
  }

  protected[this] def receiveFetchSnapshotResponse(response: SnapshotProtocol.FetchSnapshotResponse): Unit =
    response match {
      case SnapshotProtocol.SnapshotFound(snapshot) =>
        val alreadyAppliedEntries = currentData.selectAlreadyAppliedEntries(
          snapshot.metadata.entityId,
          from = snapshot.metadata.logEntryIndex.next(),
        )
        replicationActor(snapshot.metadata.entityId) ! RecoveryState(alreadyAppliedEntries, Option(snapshot))
      case SnapshotProtocol.SnapshotNotFound(entityId) =>
        val alreadyAppliedEntries = currentData.selectAlreadyAppliedEntries(entityId)
        replicationActor(entityId) ! RecoveryState(alreadyAppliedEntries, None)
    }

  protected[this] def replicationActor(entityId: NormalizedEntityId): ActorRef = {
    context.child(entityId.underlying).getOrElse {
      log.debug(
        "=== [{}] created an entity ({}) ===",
        currentState,
        entityId,
      )
      context.actorOf(replicationActorProps, entityId.underlying)
    }
  }

  override val persistenceId: String = s"raft-$typeName-${shardId.underlying}-${selfMemberIndex.role}"

  override def journalPluginId: String = settings.journalPluginId

  override def journalPluginConfig: Config = settings.journalPluginAdditionalConfig

  override def snapshotPluginId: String = settings.snapshotStorePluginId

  override def snapshotPluginConfig: Config = ConfigFactory.empty()

  private[this] def replicationId = s"$typeName-${shardId.underlying}"

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
          .appendEntries(term, logEntries, prevLogIndex)
      case AppendedEvent(event) =>
        currentData
          .appendEvent(event)
      case BecameFollower() =>
        currentData.initializeFollowerData()
      case BecameCandidate() =>
        currentData.initializeCandidateData()
      case BecameLeader() =>
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
              maybeCommitLogStore.foreach(_.save(replicationId, logEntry.index, logEntry.event.event))
            }
          }
      case Committed(logEntryIndex) =>
        currentData
          .commit(logEntryIndex)
          .handleCommittedLogEntriesAndClients { entries =>
            maybeCommitLogStore.foreach(store => {
              entries.map(_._1).foreach(logEntry => store.save(replicationId, logEntry.index, logEntry.event.event))
            })
            entries.foreach {
              case (logEntry, Some(client)) =>
                log.debug(s"=== [Leader] committed $logEntry and will notify it to $client ===")
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
        currentData.recordSavedSnapshot(metadata, settings.compactionPreserveLogSize)(onComplete = () => {
          val status = currentData.snapshottingStatus
          applyDomainEvent(
            CompactionCompleted(
              selfMemberIndex,
              shardId,
              status.snapshotLastLogTerm,
              status.snapshotLastLogIndex,
              status.completedEntities,
            ),
          ) { _ =>
            saveSnapshot(currentData.persistentState) // Note that this persistence can fail
            log.info(
              "[{}] compaction completed (term: {}, logEntryIndex: {})",
              currentState,
              status.snapshotLastLogTerm,
              status.snapshotLastLogIndex,
            )
          }
        })
      case CompactionCompleted(_, _, snapshotLastTerm, snapshotLastIndex, _) =>
        currentData.updateLastSnapshotStatus(snapshotLastTerm, snapshotLastIndex)
      case SnapshotSyncCompleted(snapshotLastLogTerm, snapshotLastLogIndex) =>
        stopAllEntities()
        currentData.syncSnapshot(snapshotLastLogTerm, snapshotLastLogIndex)
      // TODO: Remove when test code is modified
      case _: NonPersistEventLike =>
        log.error("must not use NonPersistEventLike in production code")
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

  def suspendEntity(entityId: NormalizedEntityId, stopMessage: Any): Unit = {
    log.debug(s"=== [$currentState] suspend entity '$entityId' with $stopMessage ===")
    replicationActor(entityId) ! stopMessage
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
          // do nothing
        }
      case SnapshotProtocol.SaveSnapshotFailure(_) =>
      // do nothing
    }

  private[this] var electionTimeoutTimer: Option[Cancellable] = None

  def resetElectionTimeoutTimer(): Unit = {
    cancelElectionTimeoutTimer()
    val timeout = settings.randomizedElectionTimeout()
    log.debug(s"=== [$currentState] election-timeout after ${timeout.toMillis} ms ===")
    electionTimeoutTimer = Some(context.system.scheduler.scheduleOnce(timeout, self, ElectionTimeout))
  }

  def cancelElectionTimeoutTimer(): Unit = {
    electionTimeoutTimer.foreach(_.cancel())
  }

  private[this] var heartbeatTimeoutTimer: Option[Cancellable] = None

  def resetHeartbeatTimeoutTimer(): Unit = {
    cancelHeartbeatTimeoutTimer()
    val timeout = settings.heartbeatInterval
    log.debug(s"=== [Leader] Heartbeat after ${settings.heartbeatInterval.toMillis} ms ===")
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
    log.debug(s"=== [$currentState] broadcast $message ===")
    region ! ReplicationRegion.Broadcast(message)
  }

  def applyToReplicationActor(logEntry: LogEntry): Unit =
    logEntry.event match {
      case EntityEvent(_, NoOp) => // NoOp は replicationActor には関係ないので転送しない
      case EntityEvent(Some(entityId), event) =>
        log.debug(s"=== [$currentState] applying $event to ReplicationActor ===")
        replicationActor(entityId) ! Replica(logEntry)
      case EntityEvent(None, event) =>
        log.warning(s"=== [$currentState] $event was not applied, because it is not assigned any entity ===")
    }

  def handleSnapshotTick(): Unit = {
    if (
      currentData.replicatedLog.entries.size >= settings.compactionLogSizeThreshold
      && currentData.hasLogEntriesThatCanBeCompacted
    ) {
      val (term, logEntryIndex, entityIds) = currentData.resolveSnapshotTargets()
      applyDomainEvent(SnapshottingStarted(term, logEntryIndex, entityIds)) { _ =>
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
      replicationActor(entityId) ! ReplicationActor.TakeSnapshot(metadata, self)
    }
  }

  protected def receiveInstallSnapshot(request: InstallSnapshot): Unit =
    request match {
      case installSnapshot if installSnapshot.term.isOlderThan(currentData.currentTerm) =>
      // ignore the message because this member knows another newer leader
      case installSnapshot =>
        if (installSnapshot.term == currentData.currentTerm) {
          applyDomainEvent(DetectedLeaderMember(installSnapshot.srcMemberIndex)) { _ =>
            startSyncSnapshot(installSnapshot)
            become(Follower)
          }
        } else {
          applyDomainEvent(DetectedNewTerm(installSnapshot.term)) { _ =>
            applyDomainEvent(DetectedLeaderMember(installSnapshot.srcMemberIndex)) { _ =>
              startSyncSnapshot(installSnapshot)
              become(Follower)
            }
          }
        }
    }

  protected def receiveSyncSnapshotResponse(response: SnapshotSyncManager.SyncSnapshotCompleted): Unit = {
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
  }

  protected def startSyncSnapshot(installSnapshot: InstallSnapshot): Unit = {
    val snapshotSyncManagerName = ActorIds.actorName(
      snapshotSyncManagerNamePrefix,
      installSnapshot.srcTypeName.underlying,
      installSnapshot.srcMemberIndex.role,
    )
    val snapshotSyncManager =
      context.child(snapshotSyncManagerName).getOrElse {
        context.actorOf(
          SnapshotSyncManager.props(
            srcTypeName = installSnapshot.srcTypeName,
            srcMemberIndex = installSnapshot.srcMemberIndex,
            dstTypeName = TypeName(typeName),
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

  private[this] def stopAllEntities(): Unit = {
    // FIXME: Make it possible to stop only entities by using Actor hierarchy
    val excludes: Set[String] =
      Set(ActorIds.actorName(shardSnapshotStoreNamePrefix, ""), ActorIds.actorName(snapshotSyncManagerNamePrefix, ""))
    context.children.filterNot(c => excludes.exists(c.path.name.startsWith)).foreach { child =>
      context.stop(child)
    }
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
