package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, SuspendEntity, TryCreateEntity }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor

private[raft] trait Follower { this: RaftActor =>
  import RaftActor._

  def followerBehavior: Receive = {
    case ElectionTimeout                                 => receiveElectionTimeout()
    case request: RequestVote                            => receiveRequestVote(request)
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

  private[this] def receiveElectionTimeout(): Unit = {
    if (currentData.leaderMember.isEmpty) {
      if (log.isDebugEnabled) log.debug("=== [Follower] election timeout ===")
    } else {
      if (log.isWarningEnabled) log.warning("[{}] election timeout. Leader will be changed", currentState)
    }
    cancelElectionTimeoutTimer()
    if (
      settings.stickyLeaders.isEmpty || settings.stickyLeaders
        .get(this.shardId.raw).fold(false)(_ == this.selfMemberIndex.role)
    ) {
      requestVote(currentData)
    }
  }

  private[this] def receiveRequestVote(request: RequestVote): Unit =
    request match {

      case request: RequestVote if request.term.isOlderThan(currentData.currentTerm) =>
        if (log.isDebugEnabled) log.debug("=== [Follower] deny {} ===", request)
        sender() ! RequestVoteDenied(currentData.currentTerm)

      case request: RequestVote
          if !currentData.replicatedLog.isGivenLogUpToDate(request.lastLogTerm, request.lastLogIndex) =>
        if (log.isDebugEnabled) log.debug("=== [Follower] deny {} ===", request)
        if (request.term.isNewerThan(currentData.currentTerm)) {
          applyDomainEvent(DetectedNewTerm(request.term)) { _ =>
            sender() ! RequestVoteDenied(currentData.currentTerm)
          }
        } else {
          sender() ! RequestVoteDenied(currentData.currentTerm)
        }

      case request: RequestVote if request.term.isNewerThan(currentData.currentTerm) =>
        if (log.isDebugEnabled) log.debug("=== [Follower] accept {} ===", request)
        cancelElectionTimeoutTimer()
        applyDomainEvent(Voted(request.term, request.candidate)) { domainEvent =>
          sender() ! RequestVoteAccepted(domainEvent.term, selfMemberIndex)
          resetElectionTimeoutTimer()
        }

      case request: RequestVote if !currentData.alreadyVotedOthers(request.candidate) =>
        if (log.isDebugEnabled) log.debug("=== [Follower] accept {} ===", request)
        cancelElectionTimeoutTimer()
        applyDomainEvent(Voted(request.term, request.candidate)) { domainEvent =>
          sender() ! RequestVoteAccepted(domainEvent.term, selfMemberIndex)
          resetElectionTimeoutTimer()
        }

      case request: RequestVote =>
        if (log.isDebugEnabled) log.debug("=== [Follower] deny {} ===", request)
        sender() ! RequestVoteDenied(currentData.currentTerm)
    }

  private[this] def receiveAppendEntries(request: AppendEntries): Unit =
    request match {

      case appendEntries: AppendEntries if appendEntries.term.isOlderThan(currentData.currentTerm) =>
        sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)

      case appendEntries: AppendEntries if currentData.lastSnapshotStatus.isDirty =>
        rejectAppendEntriesSinceSnapshotsAreDirty(appendEntries)

      case appendEntries: AppendEntries =>
        if (currentData.hasMatchLogEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)) {
          if (log.isDebugEnabled) log.debug("=== [Follower] append {} ===", appendEntries)
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
          if (log.isDebugEnabled) log.debug("=== [Follower] could not append {} ===", appendEntries)
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
    (currentData.leaderMember, currentData.votedFor) match {
      case (Some(leader), _) =>
        if (log.isDebugEnabled) log.debug("=== [Follower] forward {} to {} ===", command, leader)
        region forward ReplicationRegion.DeliverTo(leader, ForwardedCommand(command))
      case (None, _) =>
        stash()
    }

  private[this] def requestVote(data: RaftMemberData): Unit = {
    val newTerm = data.currentTerm.next()
    broadcast(
      RequestVote(shardId, newTerm, selfMemberIndex, data.replicatedLog.lastLogIndex, data.replicatedLog.lastLogTerm),
    ) // TODO: 永続化前に broadcast して問題ないか調べる
    applyDomainEvent(BegunNewTerm(newTerm)) { _ =>
      become(Candidate)
    }
  }

  private def receiveReplicate(replicate: Replicate): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "[Follower] cannot replicate the event: type=[{}], entityId=[{}], instanceId=[{}], entityLastAppliedIndex=[{}]",
        replicate.event.getClass.getName,
        replicate.entityId.map(_.raw),
        replicate.instanceId.map(_.underlying),
        replicate.entityLastAppliedIndex.map(_.underlying),
      )
    }
    replicate.replyTo ! ReplicationFailed
  }

}
