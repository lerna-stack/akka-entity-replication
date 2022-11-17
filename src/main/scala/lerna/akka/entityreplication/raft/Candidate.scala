package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, SuspendEntity, TryCreateEntity }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager

private[raft] trait Candidate { this: RaftActor =>
  import RaftActor._

  def candidateBehavior: Receive = {
    case ElectionTimeout                                 => receiveElectionTimeout()
    case request: RequestVote                            => receiveRequestVote(request)
    case response: RequestVoteResponse                   => receiveRequestVoteResponse(response)
    case request: AppendEntries                          => receiveAppendEntries(request)
    case request: InstallSnapshot                        => receiveInstallSnapshot(request)
    case _: InstallSnapshotResponse                      => // ignore, because I'm not a leader
    case response: SnapshotSyncManager.Response          => receiveSyncSnapshotResponse(response)
    case command: Command                                => handleCommand(command)
    case _: ForwardedCommand                             => // ignore, because I'm not a leader
    case replicate: Replicate                            => receiveReplicate(replicate)
    case TryCreateEntity(_, entityId)                    => createEntityIfNotExists(entityId)
    case request: FetchEntityEvents                      => receiveFetchEntityEvents(request)
    case EntityTerminated(id)                            => receiveEntityTerminated(id)
    case SuspendEntity(_, entityId, stopMessage)         => suspendEntity(entityId, stopMessage)
    case SnapshotTick                                    => handleSnapshotTick()
    case response: Snapshot                              => receiveEntitySnapshotResponse(response)
    case response: SnapshotProtocol.SaveSnapshotResponse => receiveSaveSnapshotResponse(response)
    case _: akka.persistence.SaveSnapshotSuccess         => // ignore
    case _: akka.persistence.SaveSnapshotFailure         => // ignore: no problem because events exist even if snapshot saving failed

    // Event sourcing protocol
    case response: CommitLogStoreActor.AppendCommittedEntriesResponse =>
      receiveAppendCommittedEntriesResponse(response)

  }

  private def receiveElectionTimeout(): Unit = {
    if (log.isInfoEnabled)
      log.info("[Candidate] Election timeout at {}. Retrying leader election.", currentData.currentTerm)
    val newTerm = currentData.currentTerm.next()
    cancelElectionTimeoutTimer()
    if (canBecomeCandidate(this.shardId, this.selfMemberIndex)) {
      broadcast(
        RequestVote(
          shardId,
          newTerm,
          selfMemberIndex,
          currentData.replicatedLog.lastLogIndex,
          currentData.replicatedLog.lastLogTerm,
        ),
      ) // TODO: 永続化前に broadcast して問題ないか調べる
      applyDomainEvent(BegunNewTerm(newTerm)) { _ =>
        become(Candidate)
      }
    }
  }

  private[this] def receiveRequestVote(request: RequestVote): Unit =
    request match {

      case RequestVote(_, term, candidate, _, _) if term == currentData.currentTerm && candidate == selfMemberIndex =>
        if (log.isDebugEnabled) log.debug("=== [Candidate] accept self RequestVote ===")
        applyDomainEvent(Voted(term, selfMemberIndex)) { _ =>
          sender() ! RequestVoteAccepted(term, selfMemberIndex)
        }

      case RequestVote(_, term, otherCandidate, lastLogIndex, lastLogTerm)
          if term.isNewerThan(currentData.currentTerm) &&
          currentData.replicatedLog.isGivenLogUpToDate(lastLogTerm, lastLogIndex) =>
        if (log.isDebugEnabled) log.debug("=== [Candidate] accept RequestVote({}, {}) ===", term, otherCandidate)
        cancelElectionTimeoutTimer()
        applyDomainEvent(Voted(term, otherCandidate)) { domainEvent =>
          sender() ! RequestVoteAccepted(domainEvent.term, selfMemberIndex)
          become(Follower)
        }

      case request: RequestVote =>
        if (log.isDebugEnabled) log.debug("=== [Candidate] deny {} ===", request)
        if (request.term.isNewerThan(currentData.currentTerm)) {
          cancelElectionTimeoutTimer()
          applyDomainEvent(DetectedNewTerm(request.term)) { _ =>
            sender() ! RequestVoteDenied(currentData.currentTerm)
            become(Follower)
          }
        } else {
          // the request has the same or old term
          sender() ! RequestVoteDenied(currentData.currentTerm)
        }
    }

  private[this] def receiveRequestVoteResponse(response: RequestVoteResponse): Unit =
    response match {

      case accepted: RequestVoteAccepted if accepted.term.isNewerThan(currentData.currentTerm) =>
        unhandled(accepted) // TODO: 不具合の可能性が高いのでエラーとして報告

      case accepted: RequestVoteAccepted if accepted.term == currentData.currentTerm =>
        cancelElectionTimeoutTimer()
        applyDomainEvent(AcceptedRequestVote(accepted.sender)) { _ =>
          if (log.isDebugEnabled) log.debug("=== [Candidate] accept for {} ===", accepted.sender)
          if (currentData.gotAcceptionMajorityOf(numberOfMembers)) {
            become(Leader)
          } else {
            resetElectionTimeoutTimer()
          }
        }

      case accepted: RequestVoteAccepted if accepted.term.isOlderThan(currentData.currentTerm) =>
      // ignore

      case accepted: RequestVoteAccepted =>
        unhandled(accepted)

      case denied: RequestVoteDenied if denied.term.isNewerThan(currentData.currentTerm) =>
        cancelElectionTimeoutTimer()
        applyDomainEvent(DetectedNewTerm(denied.term)) { _ =>
          become(Follower)
        }

      case denied: RequestVoteDenied if denied.term == currentData.currentTerm => // ignore

      case denied: RequestVoteDenied if denied.term.isOlderThan(currentData.currentTerm) =>
      // lastLogIndex が古かった場合、RequestVote が拒否される
      // 1つの Follower から拒否されたからといって Leader になれないとも限らないため単純に無視する

      case denied: RequestVoteDenied =>
        unhandled(denied)
    }

  private[this] def receiveAppendEntries(request: AppendEntries): Unit =
    request match {

      case appendEntries: AppendEntries if appendEntries.term.isOlderThan(currentData.currentTerm) =>
        sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)

      case appendEntries: AppendEntries if currentData.lastSnapshotStatus.isDirty =>
        rejectAppendEntriesSinceSnapshotsAreDirty(appendEntries)

      case appendEntries: AppendEntries =>
        if (currentData.hasMatchLogEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)) {
          if (log.isDebugEnabled) log.debug("=== [Candidate] append {} ===", appendEntries)
          cancelElectionTimeoutTimer()
          if (appendEntries.entries.isEmpty && appendEntries.term == currentData.currentTerm) {
            // do not persist event when no need
            applyDomainEvent(FollowedLeaderCommit(appendEntries.leader, appendEntries.committableIndex)) { _ =>
              sender() ! AppendEntriesSucceeded(
                appendEntries.term,
                currentData.replicatedLog.lastLogIndex,
                selfMemberIndex,
              )
              become(Follower)
            }
          } else {
            val newEntries = currentData.resolveNewLogEntries(appendEntries.entries)
            applyDomainEvent(AppendedEntries(appendEntries.term, newEntries)) { domainEvent =>
              applyDomainEvent(FollowedLeaderCommit(appendEntries.leader, appendEntries.committableIndex)) { _ =>
                sender() ! AppendEntriesSucceeded(
                  domainEvent.term,
                  currentData.replicatedLog.lastLogIndex,
                  selfMemberIndex,
                )
                become(Follower)
              }
            }
          }
        } else { // prevLogIndex と prevLogTerm がマッチするエントリが無かった
          if (log.isDebugEnabled) log.debug("=== [Candidate] could not append {} ===", appendEntries)
          cancelElectionTimeoutTimer()
          if (appendEntries.term == currentData.currentTerm) {
            applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
              sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)
              become(Follower)
            }
          } else {
            applyDomainEvent(DetectedNewTerm(appendEntries.term)) { domainEvent =>
              applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
                sender() ! AppendEntriesFailed(domainEvent.term, selfMemberIndex)
                become(Follower)
              }
            }
          }
        }
    }

  private[this] def handleCommand(command: Command): Unit =
    command match {
      case _ =>
        stash()
    }

  private def receiveReplicate(replicate: Replicate): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "[Candidate] cannot replicate the event: type=[{}], entityId=[{}], instanceId=[{}], entityLastAppliedIndex=[{}]",
        replicate.event.getClass.getName,
        replicate.entityId.map(_.raw),
        replicate.instanceId.map(_.underlying),
        replicate.entityLastAppliedIndex.map(_.underlying),
      )
    }
    replicate.replyTo ! ReplicationFailed
  }

}
