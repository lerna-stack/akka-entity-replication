package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.protocol.{ SuspendEntity, TryCreateEntity }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager
import lerna.akka.entityreplication.ReplicationRegion

private[raft] trait Follower { this: RaftActor =>
  import RaftActor._

  def followerBehavior: Receive = {

    case ElectionTimeout =>
      if (currentData.leaderMember.isEmpty) {
        if (log.isDebugEnabled) log.debug(s"=== [Follower] election timeout ===")
      } else {
        if (log.isWarningEnabled) log.warning("[{}] election timeout. Leader will be changed", currentState)
      }
      requestVote(currentData)

    case request: RequestVote                             => receiveRequestVote(request)
    case request: AppendEntries                           => receiveAppendEntries(request)
    case request: InstallSnapshot                         => receiveInstallSnapshot(request)
    case _: InstallSnapshotResponse                       => // ignore, because I'm not a leader
    case response: SnapshotSyncManager.Response           => receiveSyncSnapshotResponse(response)
    case command: Command                                 => handleCommand(command)
    case _: ForwardedCommand                              => // ignore, because I'm not a leader
    case TryCreateEntity(_, entityId)                     => createEntityIfNotExists(entityId)
    case RequestRecovery(entityId)                        => recoveryEntity(entityId)
    case response: SnapshotProtocol.FetchSnapshotResponse => receiveFetchSnapshotResponse(response)
    case SuspendEntity(_, entityId, stopMessage)          => suspendEntity(entityId, stopMessage)
    case SnapshotTick                                     => handleSnapshotTick()
    case response: Snapshot                               => receiveEntitySnapshotResponse(response)
    case response: SnapshotProtocol.SaveSnapshotResponse  => receiveSaveSnapshotResponse(response)
    case _: akka.persistence.SaveSnapshotSuccess          => // ignore
    case _: akka.persistence.SaveSnapshotFailure          => // ignore: no problem because events exist even if snapshot saving failed
  }

  private[this] def receiveRequestVote(request: RequestVote): Unit =
    request match {

      case request: RequestVote if request.term.isOlderThan(currentData.currentTerm) =>
        if (log.isDebugEnabled) log.debug(s"=== [Follower] deny $request ===")
        sender() ! RequestVoteDenied(currentData.currentTerm)

      case request: RequestVote
          if request.lastLogTerm < currentData.replicatedLog.lastLogTerm || request.lastLogIndex < currentData.replicatedLog.lastLogIndex =>
        if (log.isDebugEnabled) log.debug(s"=== [Follower] deny $request ===")
        if (request.term.isNewerThan(currentData.currentTerm)) {
          applyDomainEvent(DetectedNewTerm(request.term)) { _ =>
            sender() ! RequestVoteDenied(currentData.currentTerm)
          }
        } else {
          sender() ! RequestVoteDenied(currentData.currentTerm)
        }

      case request: RequestVote if request.term.isNewerThan(currentData.currentTerm) =>
        if (log.isDebugEnabled) log.debug(s"=== [Follower] accept $request ===")
        cancelElectionTimeoutTimer()
        applyDomainEvent(Voted(request.term, request.candidate)) { domainEvent =>
          sender() ! RequestVoteAccepted(domainEvent.term, selfMemberIndex)
          resetElectionTimeoutTimer()
        }

      case request: RequestVote if !currentData.alreadyVotedOthers(request.candidate) =>
        if (log.isDebugEnabled) log.debug(s"=== [Follower] accept $request ===")
        cancelElectionTimeoutTimer()
        applyDomainEvent(Voted(request.term, request.candidate)) { domainEvent =>
          sender() ! RequestVoteAccepted(domainEvent.term, selfMemberIndex)
          resetElectionTimeoutTimer()
        }

      case request: RequestVote =>
        if (log.isDebugEnabled) log.debug(s"=== [Follower] deny $request ===")
        sender() ! RequestVoteDenied(currentData.currentTerm)
    }

  private[this] def receiveAppendEntries(request: AppendEntries): Unit =
    request match {

      case appendEntries: AppendEntries if appendEntries.term.isOlderThan(currentData.currentTerm) =>
        sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)

      case appendEntries: AppendEntries =>
        if (currentData.hasMatchLogEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)) {
          if (log.isDebugEnabled) log.debug(s"=== [Follower] append $appendEntries ===")
          cancelElectionTimeoutTimer()
          if (appendEntries.entries.isEmpty && appendEntries.term == currentData.currentTerm) {
            // do not persist event when no need
            applyDomainEvent(FollowedLeaderCommit(appendEntries.leader, appendEntries.leaderCommit)) { _ =>
              sender() ! AppendEntriesSucceeded(
                appendEntries.term,
                currentData.replicatedLog.lastLogIndex,
                selfMemberIndex,
              )
              become(Follower)
            }
          } else {
            applyDomainEvent(AppendedEntries(appendEntries.term, appendEntries.entries, appendEntries.prevLogIndex)) {
              domainEvent =>
                applyDomainEvent(FollowedLeaderCommit(appendEntries.leader, appendEntries.leaderCommit)) { _ =>
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
          if (log.isDebugEnabled) log.debug(s"=== [Follower] could not append $appendEntries ===")
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
        if (log.isDebugEnabled) log.debug(s"=== [Follower] forward $command to $leader ===")
        region forward ReplicationRegion.DeliverTo(leader, ForwardedCommand(command))
      case (None, _) =>
        stash()
    }

  private[this] def requestVote(data: RaftMemberData): Unit = {
    val newTerm = data.currentTerm.next()
    cancelElectionTimeoutTimer()
    broadcast(
      RequestVote(shardId, newTerm, selfMemberIndex, data.replicatedLog.lastLogIndex, data.replicatedLog.lastLogTerm),
    ) // TODO: 永続化前に broadcast して問題ないか調べる
    applyDomainEvent(BegunNewTerm(newTerm)) { _ =>
      become(Candidate)
    }
  }

}
