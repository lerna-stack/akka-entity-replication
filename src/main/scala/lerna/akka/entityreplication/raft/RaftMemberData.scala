package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata

object PersistentStateData {

  final case class PersistentState(currentTerm: Term, votedFor: Option[MemberIndex], replicatedLog: ReplicatedLog)
}

trait PersistentStateData[T <: PersistentStateData[T]] {
  import PersistentStateData._

  def currentTerm: Term
  def votedFor: Option[MemberIndex]
  def replicatedLog: ReplicatedLog

  protected def updatePersistentState(
      currentTerm: Term = currentTerm,
      votedFor: Option[MemberIndex] = votedFor,
      replicatedLog: ReplicatedLog = replicatedLog,
  ): T

  def persistentState: PersistentState =
    PersistentState(currentTerm, votedFor, replicatedLog)
}

trait VolatileStateData[T <: VolatileStateData[T]] {
  def commitIndex: LogEntryIndex
  def lastApplied: LogEntryIndex
  def snapshottingStatus: SnapshottingStatus

  protected def updateVolatileState(
      commitIndex: LogEntryIndex = commitIndex,
      lastApplied: LogEntryIndex = lastApplied,
      snapshottingStatus: SnapshottingStatus = snapshottingStatus,
  ): T
}

trait FollowerData { self: RaftMemberData =>
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

  def appendEntries(term: Term, logEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex): RaftMemberData = {
    require(term >= currentTerm, s"should be term:$term >= currentTerm:$currentTerm")
    updatePersistentState(
      currentTerm = term,
      replicatedLog = replicatedLog.merge(logEntries, prevLogIndex),
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

trait CandidateData { self: RaftMemberData =>
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

trait LeaderData { self: RaftMemberData =>
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

object RaftMemberData {
  import PersistentStateData._

  def apply(persistentState: PersistentState): RaftMemberData = {
    val PersistentState(currentTerm, votedFor, replicatedLog) = persistentState
    apply(
      currentTerm = currentTerm,
      votedFor = votedFor,
      replicatedLog = replicatedLog,
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
      snapshottingStatus: SnapshottingStatus = SnapshottingStatus.empty,
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
      snapshottingStatus = snapshottingStatus,
    )
}

trait RaftMemberData
    extends PersistentStateData[RaftMemberData]
    with VolatileStateData[RaftMemberData]
    with FollowerData
    with CandidateData
    with LeaderData {

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

  def selectAlreadyAppliedEntries(
      entityId: NormalizedEntityId,
      from: LogEntryIndex = replicatedLog.headIndexOption.getOrElse(LogEntryIndex.initial()),
  ): Seq[LogEntry] = {
    replicatedLog.sliceEntries(from, to = lastApplied).filter(_.event.entityId.contains(entityId))
  }

  def alreadyVotedOthers(candidate: MemberIndex): Boolean = votedFor.fold(ifEmpty = false)(candidate != _)

  def hasMatchLogEntry(prevLogIndex: LogEntryIndex, prevLogTerm: Term): Boolean = {
    // リーダーにログが無い場合は LogEntryIndex.initial が送られてくる。
    // そのケースでは AppendEntries が成功したとみなしたいので、
    // prevLogIndex が LogEntryIndex.initial の場合はマッチするログが存在するとみなす
    prevLogIndex == LogEntryIndex.initial() || replicatedLog.get(prevLogIndex).exists(_.term == prevLogTerm)
  }

  def resolveSnapshotTargets(): (LogEntryIndex, Set[NormalizedEntityId]) = {
    (
      lastApplied,
      replicatedLog.sliceEntriesFromHead(lastApplied).flatMap(_.event.entityId.toSeq).toSet,
    )
  }

  def startSnapshotting(logEntryIndex: LogEntryIndex, entityIds: Set[NormalizedEntityId]): RaftMemberData = {
    updateVolatileState(snapshottingStatus = SnapshottingStatus(logEntryIndex, entityIds))
  }

  def recordSavedSnapshot(snapshotMetadata: EntitySnapshotMetadata, preserveLogSize: Int)(
      onComplete: () => Unit,
  ): RaftMemberData = {
    if (snapshottingStatus.isInProgress && snapshottingStatus.snapshotLastLogIndex == snapshotMetadata.logEntryIndex) {
      val newStatus =
        snapshottingStatus.recordSnapshottingComplete(snapshotMetadata.logEntryIndex, snapshotMetadata.entityId)
      if (newStatus.isCompleted) {
        onComplete()
        updateVolatileState(snapshottingStatus = newStatus)
          .updatePersistentState(replicatedLog =
            replicatedLog.deleteOldEntries(snapshottingStatus.snapshotLastLogIndex, preserveLogSize),
          )
      } else {
        updateVolatileState(snapshottingStatus = newStatus)
      }
    } else {
      this
    }
  }
}

final case class RaftMemberDataImpl(
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
    snapshottingStatus: SnapshottingStatus,
) extends RaftMemberData {

  override protected def updatePersistentState(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
      replicatedLog: ReplicatedLog,
  ): RaftMemberData =
    copy(currentTerm = currentTerm, votedFor = votedFor, replicatedLog = replicatedLog)

  override protected def updateVolatileState(
      commitIndex: LogEntryIndex,
      lastApplied: LogEntryIndex,
      snapshottingStatus: SnapshottingStatus,
  ): RaftMemberData =
    copy(
      commitIndex = commitIndex,
      votedFor = votedFor,
      lastApplied = lastApplied,
      snapshottingStatus = snapshottingStatus,
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
}
