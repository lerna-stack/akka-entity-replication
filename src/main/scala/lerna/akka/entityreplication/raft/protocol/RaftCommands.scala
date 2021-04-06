package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.NormalizedShardId
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.routing.MemberIndex

private[entityreplication] object RaftCommands {

  sealed trait RaftRequest extends ShardRequest {
    def term: Term
  }

  sealed trait RaftResponse {
    def term: Term
  }

  final case class RequestVote(
      shardId: NormalizedShardId,
      term: Term,
      candidate: MemberIndex,
      lastLogIndex: LogEntryIndex,
      lastLogTerm: Term,
  ) extends RaftRequest
      with ClusterReplicationSerializable

  sealed trait RequestVoteResponse extends RaftResponse

  final case class RequestVoteAccepted(term: Term, sender: MemberIndex)
      extends RequestVoteResponse
      with ClusterReplicationSerializable

  final case class RequestVoteDenied(term: Term) extends RequestVoteResponse with ClusterReplicationSerializable

  final case class AppendEntries(
      shardId: NormalizedShardId,
      term: Term,
      leader: MemberIndex,
      prevLogIndex: LogEntryIndex,
      prevLogTerm: Term,
      entries: Seq[LogEntry],
      leaderCommit: LogEntryIndex,
  ) extends RaftRequest
      with ClusterReplicationSerializable

  sealed trait AppendEntriesResponse extends RaftResponse

  final case class AppendEntriesSucceeded(term: Term, lastLogIndex: LogEntryIndex, sender: MemberIndex)
      extends AppendEntriesResponse
      with ClusterReplicationSerializable

  final case class AppendEntriesFailed(term: Term, sender: MemberIndex)
      extends AppendEntriesResponse
      with ClusterReplicationSerializable

  final case class InstallSnapshot(
      shardId: NormalizedShardId,
      term: Term,
      srcMemberIndex: MemberIndex,
      srcLatestSnapshotLastLogTerm: Term,
      srcLatestSnapshotLastLogLogIndex: LogEntryIndex,
  ) extends RaftRequest
      with ClusterReplicationSerializable

  sealed trait InstallSnapshotResponse extends ShardRequest

  final case class InstallSnapshotSucceeded(
      shardId: NormalizedShardId,
      term: Term,
      dstLatestSnapshotLastLogLogIndex: LogEntryIndex,
      sender: MemberIndex,
  ) extends InstallSnapshotResponse
      with ClusterReplicationSerializable
}
