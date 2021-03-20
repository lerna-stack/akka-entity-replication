package lerna.akka.entityreplication.raft

import akka.actor.ActorPath
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.protocol.{ SuspendEntity, TryCreateEntity }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager
import lerna.akka.entityreplication.{ ReplicationActor, ReplicationRegion }

trait Leader { this: RaftActor =>
  import RaftActor._

  def leaderBehavior: Receive = {

    case HeartbeatTimeout =>
      publishAppendEntries()

    case request: RequestVote                                 => receiveRequestVote(request)
    case response: RequestVoteResponse                        => ignoreRequestVoteResponse(response)
    case request: AppendEntries                               => receiveAppendEntries(request)
    case response: AppendEntriesResponse                      => receiveAppendEntriesResponse(response)
    case request: InstallSnapshot                             => receiveInstallSnapshot(request)
    case response: InstallSnapshotResponse                    => receiveInstallSnapshotResponse(response)
    case response: SnapshotSyncManager.Response               => receiveSyncSnapshotResponse(response)
    case request: Command                                     => handleCommand(request)
    case ForwardedCommand(request)                            => handleCommand(request)
    case request: Replicate                                   => replicate(request)
    case response: ReplicationResponse                        => receiveReplicationResponse(response)
    case ReplicationRegion.Passivate(entityPath, stopMessage) => startEntityPassivationProcess(entityPath, stopMessage)
    case TryCreateEntity(_, entityId)                         => createEntityIfNotExists(entityId)
    case RequestRecovery(entityId)                            => recoveryEntity(entityId)
    case response: SnapshotProtocol.FetchSnapshotResponse     => receiveFetchSnapshotResponse(response)
    case SuspendEntity(_, entityId, stopMessage)              => suspendEntity(entityId, stopMessage)
    case SnapshotTick                                         => handleSnapshotTick()
    case response: ReplicationActor.Snapshot                  => receiveEntitySnapshotResponse(response)
    case response: SnapshotProtocol.SaveSnapshotResponse      => receiveSaveSnapshotResponse(response)
    case _: akka.persistence.SaveSnapshotSuccess              => // ignore
    case _: akka.persistence.SaveSnapshotFailure              => // ignore: no problem because events exist even if snapshot saving failed
  }

  private[this] def receiveRequestVote(res: RequestVote): Unit =
    res match {

      case RequestVote(_, term, candidate, lastLogIndex, lastLogTerm)
          if term.isNewerThan(
            currentData.currentTerm,
          ) && lastLogTerm >= currentData.replicatedLog.lastLogTerm && lastLogIndex >= currentData.replicatedLog.lastLogIndex =>
        log.debug(s"=== [Leader] accept RequestVote($term, $candidate) ===")
        cancelHeartbeatTimeoutTimer()
        applyDomainEvent(Voted(term, candidate)) { domainEvent =>
          sender() ! RequestVoteAccepted(domainEvent.term, selfMemberIndex)
          become(Follower)
        }

      case request: RequestVote =>
        log.debug(s"=== [Leader] deny $request ===")
        if (request.term.isNewerThan(currentData.currentTerm)) {
          cancelHeartbeatTimeoutTimer()
          applyDomainEvent(DetectedNewTerm(request.term)) { _ =>
            sender() ! RequestVoteDenied(currentData.currentTerm)
            become(Follower)
          }
        } else {
          // the request has the same or old term
          sender() ! RequestVoteDenied(currentData.currentTerm)
        }
    }

  private[this] def ignoreRequestVoteResponse(res: RequestVoteResponse): Unit =
    res match {

      case RequestVoteAccepted(term, _) if term == currentData.currentTerm => // ignore

      case RequestVoteDenied(term) if term == currentData.currentTerm => // ignore

      case other =>
        unhandled(other) // TODO: 不具合の可能性が高いのでエラーとして報告
    }

  private[this] def receiveAppendEntries(res: AppendEntries): Unit =
    res match {

      case appendEntries: AppendEntries if appendEntries.leader == selfMemberIndex => // ignore

      case appendEntries: AppendEntries if appendEntries.term.isNewerThan(currentData.currentTerm) =>
        if (currentData.hasMatchLogEntry(appendEntries.prevLogIndex, appendEntries.prevLogTerm)) {
          cancelHeartbeatTimeoutTimer()
          log.debug(s"=== [Leader] append $appendEntries ===")
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
        } else { // prevLogIndex と prevLogTerm がマッチするエントリが無かった
          log.debug(s"=== [Leader] could not append $appendEntries ===")
          cancelHeartbeatTimeoutTimer()
          applyDomainEvent(DetectedNewTerm(appendEntries.term)) { domainEvent =>
            applyDomainEvent(DetectedLeaderMember(appendEntries.leader)) { _ =>
              sender() ! AppendEntriesFailed(domainEvent.term, selfMemberIndex)
              become(Follower)
            }
          }
        }

      case _: AppendEntries =>
        sender() ! AppendEntriesFailed(currentData.currentTerm, selfMemberIndex)
    }

  private[this] def receiveAppendEntriesResponse(res: AppendEntriesResponse): Unit =
    res match {

      case succeeded: AppendEntriesSucceeded if succeeded.term == currentData.currentTerm =>
        val follower = succeeded.sender
        applyDomainEvent(SucceededAppendEntries(follower, succeeded.lastLogIndex)) { _ =>
          val newCommitIndex = currentData.findReplicatedLastLogIndex(numberOfMembers, succeeded.lastLogIndex)
          if (newCommitIndex > currentData.commitIndex) {
            applyDomainEvent(Committed(newCommitIndex)) { _ =>
              // release stashed commands
              unstashAll()
            }
          }
        }

      case succeeded: AppendEntriesSucceeded if succeeded.term.isNewerThan(currentData.currentTerm) =>
        log.warning("Unexpected message received: {} (currentTerm: {})", succeeded, currentData.currentTerm)

      case succeeded: AppendEntriesSucceeded if succeeded.term.isOlderThan(currentData.currentTerm) =>
      // ignore: Follower always synchronizes Term before replying, so it does not happen normally

      case succeeded: AppendEntriesSucceeded =>
        unhandled(succeeded)

      case failed: AppendEntriesFailed if failed.term == currentData.currentTerm =>
        applyDomainEvent(DeniedAppendEntries(failed.sender)) { _ =>
          // do nothing
        }

      case failed: AppendEntriesFailed if failed.term.isNewerThan(currentData.currentTerm) =>
        cancelHeartbeatTimeoutTimer()
        applyDomainEvent(DetectedNewTerm(failed.term)) { _ =>
          become(Follower)
        }

      case failed: AppendEntriesFailed if failed.term.isOlderThan(currentData.currentTerm) => // ignore

      case failed: AppendEntriesFailed =>
        unhandled(failed)
    }

  private[this] def receiveInstallSnapshotResponse(response: InstallSnapshotResponse): Unit =
    response match {
      case succeeded: InstallSnapshotSucceeded if succeeded.term == currentData.currentTerm =>
        val follower = succeeded.sender
        applyDomainEvent(SucceededAppendEntries(follower, succeeded.dstLatestSnapshotLastLogLogIndex)) { _ => }

      case succeeded: InstallSnapshotSucceeded if succeeded.term.isNewerThan(currentData.currentTerm) =>
        log.warning("Unexpected message received: {} (currentTerm: {})", succeeded, currentData.currentTerm)

      case succeeded: InstallSnapshotSucceeded =>
        assert(succeeded.term.isOlderThan(currentData.currentTerm))
      // ignore: Snapshot synchronization of Follower was too slow
    }

  private[this] def handleCommand(req: Command): Unit =
    req match {

      case Command(message) =>
        if (currentData.currentTermIsCommitted) {
          val (entityId, cmd) = extractEntityId(message)
          broadcast(TryCreateEntity(shardId, entityId))
          replicationActor(entityId) forward Command(cmd)
        } else {
          // The commands will be released after initial NoOp event was committed
          stash()
        }
    }

  private[this] def replicate(replicate: Replicate): Unit = {
    cancelHeartbeatTimeoutTimer()
    applyDomainEvent(AppendedEvent(EntityEvent(replicate.entityId, replicate.event))) { _ =>
      applyDomainEvent(
        StartedReplication(
          ClientContext(replicate.replyTo, replicate.instanceId, replicate.originSender),
          currentData.replicatedLog.lastLogIndex,
        ),
      ) { _ =>
        publishAppendEntries()
      }
    }
  }

  private[this] def receiveReplicationResponse(event: ReplicationResponse): Unit =
    event match {

      case ReplicationSucceeded(NoOp, _, _) =>
      // ignore: no-op replication when become leader

      case ReplicationSucceeded(unknownEvent, _, _) =>
        log.warning("unknown event: {}", unknownEvent)
    }

  private[this] def startEntityPassivationProcess(entityPath: ActorPath, stopMessage: Any): Unit = {
    broadcast(SuspendEntity(shardId, NormalizedEntityId.of(entityPath), stopMessage))
  }

  private[this] def publishAppendEntries(): Unit = {
    resetHeartbeatTimeoutTimer()
    otherMemberIndexes.foreach { memberIndex =>
      val nextIndex    = currentData.nextIndexFor(memberIndex)
      val prevLogIndex = nextIndex.prev()
      val prevLogTerm  = currentData.replicatedLog.termAt(prevLogIndex)
      val messages =
        prevLogTerm match {
          case Some(prevLogTerm) =>
            val batchEntries = currentData.replicatedLog.getFrom(
              nextIndex,
              settings.maxAppendEntriesSize,
              settings.maxAppendEntriesBatchSize,
            )
            batchEntries match {
              case batchEntries if batchEntries.isEmpty =>
                Seq(
                  AppendEntries(
                    shardId,
                    currentData.currentTerm,
                    selfMemberIndex,
                    prevLogIndex,
                    prevLogTerm,
                    entries = Seq.empty,
                    currentData.commitIndex,
                  ),
                )
              case batchEntries =>
                batchEntries.map { entries =>
                  AppendEntries(
                    shardId,
                    currentData.currentTerm,
                    selfMemberIndex,
                    prevLogIndex,
                    prevLogTerm,
                    entries,
                    currentData.commitIndex,
                  )
                }
            }
          case None =>
            // prevLogTerm not found: the log entries have been removed by compaction
            Seq(
              InstallSnapshot(
                shardId,
                currentData.currentTerm,
                selfMemberIndex,
                srcLatestSnapshotLastLogTerm = currentData.lastSnapshotStatus.snapshotLastTerm,
                srcLatestSnapshotLastLogLogIndex = currentData.lastSnapshotStatus.snapshotLastLogIndex,
              ),
            )
        }
      log.debug(s"=== [Leader] publish ${messages.mkString(",")} to $memberIndex ===")
      messages.foreach(region ! ReplicationRegion.DeliverTo(memberIndex, _))
    }
  }
}
