package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.NormalizedShardId
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.routing.MemberIndex

object RaftCommands {

  sealed trait RaftRequest extends ShardRequest {
    def term: Term
  }

  sealed trait RaftResponse {
    def term: Term
  }

  case class RequestVote(
      shardId: NormalizedShardId,
      term: Term,
      candidate: MemberIndex,
      lastLogIndex: LogEntryIndex,
      lastLogTerm: Term,
  ) extends RaftRequest

  sealed trait RequestVoteResponse extends RaftResponse

  case class RequestVoteAccepted(term: Term, sender: MemberIndex)
      extends RequestVoteResponse
      with ClusterReplicationSerializable

  case class RequestVoteDenied(term: Term) extends RequestVoteResponse with ClusterReplicationSerializable

  case class AppendEntries(
      shardId: NormalizedShardId,
      term: Term,
      leader: MemberIndex,
      prevLogIndex: LogEntryIndex,
      prevLogTerm: Term,
      entries: Seq[LogEntry],
      leaderCommit: LogEntryIndex,
  ) extends RaftRequest

  sealed trait AppendEntriesResponse extends RaftResponse

  case class AppendEntriesSucceeded(term: Term, lastLogIndex: LogEntryIndex, sender: MemberIndex)
      extends AppendEntriesResponse
      with ClusterReplicationSerializable

  case class AppendEntriesFailed(term: Term, sender: MemberIndex) extends AppendEntriesResponse
}
