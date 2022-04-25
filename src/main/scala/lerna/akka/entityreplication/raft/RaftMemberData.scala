package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata
import org.slf4j.{ Logger, LoggerFactory }

private[entityreplication] object PersistentStateData {

  final case class PersistentState(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
      replicatedLog: ReplicatedLog,
      lastSnapshotStatus: SnapshotStatus,
  ) extends ClusterReplicationSerializable
}

private[entityreplication] trait PersistentStateData[T <: PersistentStateData[T]] {
  import PersistentStateData._

  def currentTerm: Term
  def votedFor: Option[MemberIndex]
  def replicatedLog: ReplicatedLog
  def lastSnapshotStatus: SnapshotStatus

  protected def updatePersistentState(
      currentTerm: Term = currentTerm,
      votedFor: Option[MemberIndex] = votedFor,
      replicatedLog: ReplicatedLog = replicatedLog,
      lastSnapshotStatus: SnapshotStatus = lastSnapshotStatus,
  ): T

  def persistentState: PersistentState =
    PersistentState(currentTerm, votedFor, replicatedLog, lastSnapshotStatus)
}

private[entityreplication] trait VolatileStateData[T <: VolatileStateData[T]] {
  def commitIndex: LogEntryIndex
  def lastApplied: LogEntryIndex
  def snapshottingProgress: SnapshottingProgress

  /** Indicates that [[eventsourced.CommitLogStoreActor]] has already persisted events with indices
    * lower than or equal to that index
    *
    * Note that `None` is '''NOT''' the same as `Some(LogEntryIndex(0))`.
    * `None` indicates that a Raft actor doesn't know such an index.
    * `Some(LogEntryIndex(0))` indicates that a Raft actor know that such an index is zero.
    *
    * [[eventSourcingIndex]] may be larger than [[commitIndex]] since the leader election and the event sourcing works
    * independently. This case happens if there is no leader (due to a split vote) until a follower update its [[eventSourcingIndex]]
    * at SnapshotTick.
    */
  def eventSourcingIndex: Option[LogEntryIndex]

  protected def updateVolatileState(
      commitIndex: LogEntryIndex = commitIndex,
      lastApplied: LogEntryIndex = lastApplied,
      snapshottingProgress: SnapshottingProgress = snapshottingProgress,
      eventSourcingIndex: Option[LogEntryIndex] = eventSourcingIndex,
  ): T

  /** Returns new [[RaftMemberData]] those [[eventSourcingIndex]] updated to the given index
    *
    * [[eventSourcingIndex]] should only increase.
    * If the given index is lower than the current [[eventSourcingIndex]], throws an [[IllegalArgumentException]].
    * This method always success if the current [[eventSourcingIndex]] is [[None]].
    */
  def updateEventSourcingIndex(newEventSourcingIndex: LogEntryIndex): T = {
    eventSourcingIndex.foreach { currentEventSourcingIndex =>
      require(
        currentEventSourcingIndex < newEventSourcingIndex,
        "eventSourcingIndex should only increase. " +
        s"The given index [${newEventSourcingIndex.underlying}] is less than or equal to the current index [${currentEventSourcingIndex.underlying}].",
      )
    }
    updateVolatileState(eventSourcingIndex = Option(newEventSourcingIndex))
  }

}

private[entityreplication] trait FollowerData { self: RaftMemberData =>
  def leaderMember: Option[MemberIndex]

  def initializeFollowerData(): RaftMemberData = {
    this
  }

  def syncTerm(term: Term): RaftMemberData = {
    require(term >= currentTerm, s"should be term:$term >= currentTerm:$currentTerm")
    if (term == currentTerm) {
      updatePersistentState(currentTerm = term)
    } else {
      updatePersistentState(currentTerm = term, votedFor = None)
    }
  }

  def vote(candidate: MemberIndex, term: Term): RaftMemberData = {
    require(
      !(term < currentTerm),
      s"term:$term should be greater than or equal to currentTerm:$currentTerm",
    )

    updatePersistentState(currentTerm = term, votedFor = Some(candidate))
  }

  /** Returns new entries that contain no existing entries
    *
    * While the given entries could conflict with the existing entries, this method finds such a conflict and returns entries
    * beginning with the conflicting. [[ReplicatedLog.findConflict]] describes what conflicting means.
    *
    * The given entries should start with an index less than or equal to the last index of exising entries plus one.
    * If this requirement breaks, this method throws an [[IllegalArgumentException]] since it will miss some entries.
    *
    * Note that the index of the given entries MUST be continuously increasing (not checked on this method)
    *
    * @see [[ReplicatedLog.findConflict]]
    */
  def resolveNewLogEntries(logEntries: Seq[LogEntry]): Seq[LogEntry] = {
    import ReplicatedLog.FindConflictResult
    require(
      logEntries.isEmpty || logEntries.head.index <= replicatedLog.lastLogIndex.plus(1),
      s"The given non-empty log entries (indices: [${logEntries.head.index}..${logEntries.last.index}]) should start with an index " +
      s"less than or equal to lastLogIndex[${replicatedLog.lastLogIndex}] + 1. " +
      "If this requirement breaks, the raft log will miss some entries.",
    )
    replicatedLog.findConflict(logEntries) match {
      case FindConflictResult.NoConflict =>
        logEntries
          .dropWhile(_.index <= replicatedLog.lastLogIndex)
          .ensuring(
            _.headOption.forall(_.index == replicatedLog.lastLogIndex.plus(1)),
            s"The new entries (not-empty) should start with lastLogIndex ${replicatedLog.lastLogIndex} + 1.",
          )
      case FindConflictResult.ConflictFound(conflictIndex, conflictTerm) =>
        if (log.isInfoEnabled) {
          log.info(
            "found conflict at index [{}] (existing term: [{}], conflicting term: [{}]).",
            conflictIndex,
            replicatedLog.termAt(conflictIndex),
            conflictTerm,
          )
        }
        assert(
          conflictIndex > commitIndex,
          s"The entry with index [$conflictIndex] should not conflict with the committed entry (commitIndex [$commitIndex])",
        )
        assert(
          conflictIndex > replicatedLog.ancestorLastIndex,
          s"The entry with index [$conflictIndex] should not conflict with the compacted entry (ancestorLastIndex [${replicatedLog.ancestorLastIndex}])",
        )
        assert(
          conflictIndex <= replicatedLog.lastLogIndex,
          s"The entry with index [$conflictIndex] should conflict with an exising entry, but didn't (lastLogIndex [${replicatedLog.lastLogIndex}])",
        )
        val newEntries = logEntries
          .dropWhile(_.index < conflictIndex)
        assert(
          newEntries.nonEmpty && newEntries.head.index == conflictIndex,
          s"The new entries (containing conflicts, size=[${newEntries.size}]) should always be non-empty and start with the conflict Index [$conflictIndex]",
        )
        newEntries
    }
  }

  @deprecated("Use RaftMemberData.truncateAndAppendEntries instead.", "2.1.1")
  def appendEntries(logEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex): RaftMemberData = {
    updatePersistentState(
      replicatedLog = replicatedLog.merge(logEntries, prevLogIndex),
    )
  }

  def truncateAndAppendEntries(logEntries: Seq[LogEntry]): RaftMemberData = {
    updatePersistentState(
      replicatedLog = replicatedLog.truncateAndAppend(logEntries),
    )
  }

  def detectLeaderMember(leaderMember: MemberIndex): RaftMemberData = {
    updateFollowerVolatileState(leaderMember = Some(leaderMember))
  }

  def followLeaderCommit(leaderCommit: LogEntryIndex): RaftMemberData = {
    if (leaderCommit >= commitIndex) {
      import LogEntryIndex.min
      val newCommitIndex = replicatedLog.lastIndexOption
        .map { lastIndex =>
          if (leaderCommit > commitIndex) min(leaderCommit, lastIndex) else commitIndex
        }.getOrElse(commitIndex)
      updateVolatileState(commitIndex = newCommitIndex)
    } else {
      // If a new leader is elected even if the leader is alive,
      // leaderCommit is less than commitIndex when the old leader didn't tell the follower the new commitIndex.
      // Do not back commitIndex because there is a risk of applying the event to Entity in duplicate.
      this
    }
  }

  protected def updateFollowerVolatileState(leaderMember: Option[MemberIndex] = leaderMember): RaftMemberData
}

private[entityreplication] trait CandidateData { self: RaftMemberData =>
  def acceptedMembers: Set[MemberIndex]

  def initializeCandidateData(): RaftMemberData = {
    updateFollowerVolatileState(
      leaderMember = None,
    ).updateCandidateVolatileState(
      acceptedMembers = Set(),
    )
  }

  def acceptedBy(follower: MemberIndex): RaftMemberData = {
    updateCandidateVolatileState(acceptedMembers = acceptedMembers + follower)
  }

  def gotAcceptionMajorityOf(numberOfMembers: Int): Boolean =
    acceptedMembers.size >= (numberOfMembers / 2) + 1

  protected def updateCandidateVolatileState(acceptedMembers: Set[MemberIndex]): RaftMemberData
}

private[entityreplication] trait LeaderData { self: RaftMemberData =>
  def nextIndex: Option[NextIndex]
  def matchIndex: MatchIndex
  def clients: Map[LogEntryIndex, ClientContext]

  private[this] def getNextIndex: NextIndex =
    nextIndex.getOrElse(throw new IllegalStateException("nextIndex does not initialized"))

  def initializeLeaderData(): RaftMemberData = {
    updateLeaderVolatileState(
      nextIndex = Some(NextIndex(replicatedLog)),
      matchIndex = MatchIndex(),
    )
  }

  def appendEvent(event: EntityEvent): RaftMemberData = {
    updatePersistentState(replicatedLog = replicatedLog.append(event, currentTerm))
  }

  def registerClient(client: ClientContext, logEntryIndex: LogEntryIndex): RaftMemberData = {
    updateLeaderVolatileState(clients = clients + (logEntryIndex -> client))
  }

  def nextIndexFor(follower: MemberIndex): LogEntryIndex = {
    getNextIndex(follower)
  }

  def syncLastLogIndex(follower: MemberIndex, lastLogIndex: LogEntryIndex): RaftMemberData = {
    updateLeaderVolatileState(
      nextIndex = Some(getNextIndex.update(follower, lastLogIndex.next())),
      matchIndex = matchIndex.update(follower, lastLogIndex),
    )
  }

  def markSyncLogFailed(follower: MemberIndex): RaftMemberData = {
    val followerNextIndex = getNextIndex(follower).prev()
    updateLeaderVolatileState(nextIndex = Some(getNextIndex.update(follower, followerNextIndex)))
  }

  /**
    * @param numberOfAllMembers
    * @param maxIndex
    * @return If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm
    *         (N <= maxIndex)
    *         - true: N
    *         - false: commitIndex
    */
  def findReplicatedLastLogIndex(numberOfAllMembers: Int, maxIndex: LogEntryIndex): LogEntryIndex = {
    val numberOfMajorityMembers = (numberOfAllMembers / 2) + 1
    replicatedLog
      .sliceEntries(from = commitIndex.next(), to = maxIndex).reverse.find { entry =>
        val leaderMatchIndexCount   = 1
        val followerMatchIndexCount = matchIndex.countMatch(_ >= entry.index)
        val matchIndexCount         = leaderMatchIndexCount + followerMatchIndexCount
        // true: 閾値までレプリケーションできた false: まだレプリケーションできていない
        entry.term == currentTerm && matchIndexCount >= numberOfMajorityMembers
      }.map(_.index).getOrElse(commitIndex)
  }

  def commit(logEntryIndex: LogEntryIndex): RaftMemberData = {
    require(logEntryIndex >= commitIndex)
    updateVolatileState(commitIndex = logEntryIndex)
  }

  def currentTermIsCommitted: Boolean = {
    val commitIndexTerm = replicatedLog.get(commitIndex).map(_.term)
    commitIndexTerm.contains(currentTerm)
  }

  def handleCommittedLogEntriesAndClients(handler: Seq[(LogEntry, Option[ClientContext])] => Unit): RaftMemberData = {
    val applicableLogEntries = selectApplicableLogEntries
    handler(applicableLogEntries.map(e => (e, clients.get(e.index))))
    updateVolatileState(lastApplied = applicableLogEntries.lastOption.map(_.index).getOrElse(lastApplied))
      .updateLeaderVolatileState(clients = clients -- applicableLogEntries.map(_.index)) // 通知したクライアントは削除してメモリを節約
  }

  protected def updateLeaderVolatileState(
      nextIndex: Option[NextIndex] = nextIndex,
      matchIndex: MatchIndex = matchIndex,
      clients: Map[LogEntryIndex, ClientContext] = clients,
  ): RaftMemberData
}

private[entityreplication] object ShardData {

  type EntityStates = Map[NormalizedEntityId, EntityState]

  sealed trait EntityState {
    def isPassivating: Boolean
  }
  final case object NoState extends EntityState {
    override def isPassivating: Boolean = false
  }
  final case object Passivating extends EntityState {
    override def isPassivating: Boolean = true
  }

}

private[entityreplication] trait ShardData { self: RaftMemberData =>
  import ShardData._

  def entityStates: EntityStates

  def entityStateOf(entityId: NormalizedEntityId): EntityState = {
    entityStates.getOrElse(entityId, NoState)
  }

  def passivateEntity(entityId: NormalizedEntityId): RaftMemberData =
    updateShardVolatileState(
      entityStates = entityStates.updated(entityId, Passivating),
    )

  def terminateEntity(entityId: NormalizedEntityId): RaftMemberData =
    updateShardVolatileState(
      entityStates = entityStates.removed(entityId),
    )

  protected def updateShardVolatileState(
      entityStates: EntityStates = entityStates,
  ): RaftMemberData
}

private[entityreplication] object RaftMemberData {
  import PersistentStateData._

  def apply(persistentState: PersistentState): RaftMemberData = {
    val PersistentState(currentTerm, votedFor, replicatedLog, snapshotStatus) = persistentState
    apply(
      currentTerm = currentTerm,
      votedFor = votedFor,
      replicatedLog = replicatedLog,
      lastSnapshotStatus = snapshotStatus,
    )
  }

  def apply(
      currentTerm: Term = Term.initial(),
      votedFor: Option[MemberIndex] = None,
      replicatedLog: ReplicatedLog = ReplicatedLog(),
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      lastApplied: LogEntryIndex = LogEntryIndex.initial(),
      leaderMember: Option[MemberIndex] = None,
      acceptedMembers: Set[MemberIndex] = Set(),
      nextIndex: Option[NextIndex] = None,
      matchIndex: MatchIndex = MatchIndex(),
      clients: Map[LogEntryIndex, ClientContext] = Map(),
      snapshottingProgress: SnapshottingProgress = SnapshottingProgress.empty,
      lastSnapshotStatus: SnapshotStatus = SnapshotStatus.empty,
      entityStates: ShardData.EntityStates = Map(),
      eventSourcingIndex: Option[LogEntryIndex] = None,
  ) =
    RaftMemberDataImpl(
      currentTerm = currentTerm,
      votedFor = votedFor,
      replicatedLog = replicatedLog,
      commitIndex = commitIndex,
      lastApplied = lastApplied,
      leaderMember = leaderMember,
      acceptedMembers = acceptedMembers,
      nextIndex = nextIndex,
      matchIndex = matchIndex,
      clients = clients,
      snapshottingProgress = snapshottingProgress,
      lastSnapshotStatus = lastSnapshotStatus,
      entityStates = entityStates,
      eventSourcingIndex = eventSourcingIndex,
    )

  /** Indicates an error reason [[RaftMemberData.resolveCommittedEntriesForEventSourcing]] returns */
  sealed trait CommittedEntriesForEventSourcingResolveError
  object CommittedEntriesForEventSourcingResolveError {

    /** Indicates [[RaftMemberData]] doesn't have the index of entries [[eventsourced.CommitLogStoreActor]] has saved
      * ([[RaftMemberData.eventSourcingIndex]] is [[None]]
      */
    case object UnknownCurrentEventSourcingIndex extends CommittedEntriesForEventSourcingResolveError

    /** Indicates [[RaftMemberData.replicatedLog]] doesn't contain the next entry even if there should be.
      *
      * The next entry should have the next eventSourcingIndex ([[RaftMemberData.eventSourcingIndex]] + one).
      */
    final case class NextCommittedEntryNotFound(
        nextEventSourcingIndex: LogEntryIndex,
        foundFirstIndex: Option[LogEntryIndex],
    ) extends CommittedEntriesForEventSourcingResolveError
  }

}

private[entityreplication] trait RaftMemberData
    extends PersistentStateData[RaftMemberData]
    with VolatileStateData[RaftMemberData]
    with FollowerData
    with CandidateData
    with LeaderData
    with ShardData {

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  protected def selectApplicableLogEntries: Seq[LogEntry] =
    if (commitIndex > lastApplied) {
      replicatedLog.sliceEntries(from = lastApplied.next(), to = commitIndex)
    } else {
      Seq.empty
    }

  def applyCommittedLogEntries(handler: Seq[LogEntry] => Unit): RaftMemberData = {
    val applicableLogEntries = selectApplicableLogEntries
    handler(applicableLogEntries)
    updateVolatileState(lastApplied = applicableLogEntries.lastOption.map(_.index).getOrElse(lastApplied))
  }

  def selectEntityEntries(
      entityId: NormalizedEntityId,
      from: LogEntryIndex,
      to: LogEntryIndex,
  ): Seq[LogEntry] = {
    require(
      to <= lastApplied,
      s"Cannot select the entries (${from}-${to}) unless RaftActor have applied the entries to the entities (lastApplied: ${lastApplied})",
    )
    replicatedLog.sliceEntries(from, to).filter(_.event.entityId.contains(entityId))
  }

  def hasUncommittedLogEntryOf(entityId: NormalizedEntityId): Boolean = {
    replicatedLog
      .entriesAfter(index = commitIndex) // uncommitted entries
      .exists(_.event.entityId.contains(entityId))
  }

  def alreadyVotedOthers(candidate: MemberIndex): Boolean = votedFor.exists(candidate != _)

  def hasMatchLogEntry(prevLogIndex: LogEntryIndex, prevLogTerm: Term): Boolean = {
    // リーダーにログが無い場合は LogEntryIndex.initial が送られてくる。
    // そのケースでは AppendEntries が成功したとみなしたいので、
    // prevLogIndex が LogEntryIndex.initial の場合はマッチするログが存在するとみなす
    prevLogIndex == LogEntryIndex.initial() || replicatedLog.termAt(prevLogIndex).contains(prevLogTerm)
  }

  def willGetMatchSnapshots(prevLogIndex: LogEntryIndex, prevLogTerm: Term): Boolean = {
    prevLogTerm == lastSnapshotStatus.targetSnapshotLastTerm &&
    prevLogIndex == lastSnapshotStatus.targetSnapshotLastLogIndex
  }

  /** Returns true if [[replicatedLog]] has entries that have been already applied */
  def hasAppliedLogEntries: Boolean = {
    replicatedLog.sliceEntriesFromHead(lastApplied).nonEmpty
  }

  def resolveSnapshotTargets(): (Term, LogEntryIndex, Set[NormalizedEntityId]) = {
    replicatedLog.termAt(lastApplied) match {
      case Some(lastAppliedTerm) =>
        val entityIds =
          replicatedLog
            .sliceEntries(lastSnapshotStatus.snapshotLastLogIndex.next(), lastApplied)
            .flatMap(_.event.entityId.toSeq)
            .toSet
        (lastAppliedTerm, lastApplied, entityIds)
      case None =>
        // This exception is not thrown unless there is a bug
        throw new IllegalStateException(s"Term not found at lastApplied: $lastApplied")
    }
  }

  def startSnapshotting(
      term: Term,
      logEntryIndex: LogEntryIndex,
      entityIds: Set[NormalizedEntityId],
  ): RaftMemberData = {
    updateVolatileState(snapshottingProgress =
      SnapshottingProgress(term, logEntryIndex, inProgressEntities = entityIds, completedEntities = Set()),
    )
  }

  def recordSavedSnapshot(snapshotMetadata: EntitySnapshotMetadata): RaftMemberData = {
    if (
      snapshottingProgress.isInProgress && snapshottingProgress.snapshotLastLogIndex == snapshotMetadata.logEntryIndex
    ) {
      val newProgress =
        snapshottingProgress.recordSnapshottingComplete(snapshotMetadata.logEntryIndex, snapshotMetadata.entityId)
      updateVolatileState(snapshottingProgress = newProgress)
    } else {
      this
    }
  }

  def updateLastSnapshotStatus(snapshotLastTerm: Term, snapshotLastIndex: LogEntryIndex): RaftMemberData = {
    updatePersistentState(lastSnapshotStatus =
      lastSnapshotStatus.updateSnapshotsCompletely(snapshotLastTerm, snapshotLastIndex),
    )
  }

  /** Returns the estimated size of [[replicatedLog]] after compaction completes.
    *
    * This estimated size is helpful to decide whether the compaction executes or not.
    * Note that this value is '''estimation''' since values for the calculation can change during compaction.
    *
    * Throws an [[IllegalArgumentException]] if the given `preserveLogSize` is less than or equals to 0.
    */
  def estimatedReplicatedLogSizeAfterCompaction(preserveLogSize: Int): Int = {
    require(preserveLogSize > 0, s"preserveLogSize($preserveLogSize) should be greater than 0.")
    val toIndex = LogEntryIndex.min(
      lastApplied,
      eventSourcingIndex.getOrElse(LogEntryIndex(0)),
    )
    replicatedLog.deleteOldEntries(toIndex, preserveLogSize).entries.size
  }

  /** Return new [[RaftMemberData]] those [[replicatedLog]] compacted (some prefix entries are deleted)
    *
    * While preserving that the compacted log has at least the given `preserveLogSize` entries, the compaction deletes
    * entries with indices less than or equal to the minimum of index of [[lastSnapshotStatus]] and [[eventSourcingIndex]].
    * If [[eventSourcingIndex]] is unknown (it is [[None]]), the compaction deletes no entries.
    * The compacted log entries might be less than `preserveLogSize` if the current number of entries is already smaller than that size.
    *
    * Throws an [[IllegalArgumentException]] if the given `preserveLogSize` is less than or equals to 0.
    */
  def compactReplicatedLog(preserveLogSize: Int): RaftMemberData = {
    require(preserveLogSize > 0, s"preserveLogSize($preserveLogSize) should be greater than 0.")
    val toIndex = LogEntryIndex.min(
      lastSnapshotStatus.snapshotLastLogIndex,
      // Use 0 as the default since this must not delete entries if eventSourcingIndex is unknown.
      eventSourcingIndex.getOrElse(LogEntryIndex(0)),
    )
    updatePersistentState(
      replicatedLog = replicatedLog.deleteOldEntries(toIndex, preserveLogSize),
    )
  }

  def startSnapshotSync(snapshotLastLogTerm: Term, snapshotLastLogIndex: LogEntryIndex): RaftMemberData = {
    updatePersistentState(
      lastSnapshotStatus = lastSnapshotStatus.startSnapshotSync(snapshotLastLogTerm, snapshotLastLogIndex),
    )
  }

  def completeSnapshotSync(snapshotLastLogTerm: Term, snapshotLastLogIndex: LogEntryIndex): RaftMemberData = {
    updatePersistentState(
      /**
        * [[startSnapshotSync()]] updates [[SnapshotStatus.snapshotLastTerm]] and [[SnapshotStatus.snapshotLastLogIndex]]
        * but we updates these value again here for backward-compatibility.
        * Because the event sequence produced by v2.0.0 doesn't call [[startSnapshotSync()]].
        */
      lastSnapshotStatus = lastSnapshotStatus.updateSnapshotsCompletely(snapshotLastLogTerm, snapshotLastLogIndex),
      replicatedLog = replicatedLog.reset(snapshotLastLogTerm, snapshotLastLogIndex),
    )
  }

  /** Returns a sequence of [[LogEntry]] to persist in [[eventsourced.CommitLogStoreActor]]
    *
    * Returns [[RaftMemberData.CommittedEntriesForEventSourcingResolveError.UnknownCurrentEventSourcingIndex]] if this
    * data doesn't have the index of entries `CommitLogStoreActor` has saved ([[eventSourcingIndex]] is [[None]]).
    *
    * If [[eventSourcingIndex]] is less than [[commitIndex]], returning entries should not be empty, and should contain
    * the entry with eventSourcingIndex plus one. Returns [[RaftMemberData.CommittedEntriesForEventSourcingResolveError.NextCommittedEntryNotFound]]
    * if these conditions don't meet.
    *
    * Note that [[eventSourcingIndex]] may be larger than [[commitIndex]]. See [[eventSourcingIndex]].
    */
  def resolveCommittedEntriesForEventSourcing
      : Either[RaftMemberData.CommittedEntriesForEventSourcingResolveError, IndexedSeq[LogEntry]] = {
    import RaftMemberData.CommittedEntriesForEventSourcingResolveError._
    eventSourcingIndex match {
      case None =>
        Left(UnknownCurrentEventSourcingIndex)
      case Some(currentEventSourcingIndex) =>
        if (currentEventSourcingIndex < commitIndex) {
          val nextEventSourcingIndex = currentEventSourcingIndex.next()
          val availableEntries =
            replicatedLog.sliceEntries(from = nextEventSourcingIndex, to = commitIndex)
          val firstIndexOption = availableEntries.headOption.map(_.index)
          if (firstIndexOption != Option(nextEventSourcingIndex)) {
            Left(NextCommittedEntryNotFound(nextEventSourcingIndex, firstIndexOption))
          } else {
            Right(availableEntries.toIndexedSeq)
          }
        } else {
          Right(IndexedSeq.empty)
        }
    }
  }

}

private[entityreplication] final case class RaftMemberDataImpl(
    currentTerm: Term,
    votedFor: Option[MemberIndex],
    replicatedLog: ReplicatedLog,
    commitIndex: LogEntryIndex,
    lastApplied: LogEntryIndex,
    leaderMember: Option[MemberIndex],
    acceptedMembers: Set[MemberIndex],
    nextIndex: Option[NextIndex],
    matchIndex: MatchIndex,
    clients: Map[LogEntryIndex, ClientContext],
    snapshottingProgress: SnapshottingProgress,
    lastSnapshotStatus: SnapshotStatus,
    entityStates: ShardData.EntityStates,
    eventSourcingIndex: Option[LogEntryIndex],
) extends RaftMemberData {

  override protected def updatePersistentState(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
      replicatedLog: ReplicatedLog,
      lastSnapshotStatus: SnapshotStatus,
  ): RaftMemberData =
    copy(
      currentTerm = currentTerm,
      votedFor = votedFor,
      replicatedLog = replicatedLog,
      lastSnapshotStatus = lastSnapshotStatus,
    )

  override protected def updateVolatileState(
      commitIndex: LogEntryIndex,
      lastApplied: LogEntryIndex,
      snapshottingProgress: SnapshottingProgress,
      eventSourcingIndex: Option[LogEntryIndex],
  ): RaftMemberData =
    copy(
      commitIndex = commitIndex,
      votedFor = votedFor,
      lastApplied = lastApplied,
      snapshottingProgress = snapshottingProgress,
      eventSourcingIndex = eventSourcingIndex,
    )

  override protected def updateFollowerVolatileState(leaderMember: Option[MemberIndex]): RaftMemberData =
    copy(leaderMember = leaderMember)

  override protected def updateCandidateVolatileState(acceptedMembers: Set[MemberIndex]): RaftMemberData =
    copy(acceptedMembers = acceptedMembers)

  override protected def updateLeaderVolatileState(
      nextIndex: Option[NextIndex],
      matchIndex: MatchIndex,
      clients: Map[LogEntryIndex, ClientContext],
  ): RaftMemberData =
    copy(nextIndex = nextIndex, matchIndex = matchIndex, clients = clients)

  override protected def updateShardVolatileState(
      entityStates: ShardData.EntityStates,
  ): RaftMemberData =
    copy(entityStates = entityStates)
}
