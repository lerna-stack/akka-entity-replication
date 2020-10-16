package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._

class RaftActorLeaderSpec extends TestKit(ActorSystem()) with RaftActorSpecBase {

  import RaftActor._

  private[this] val entityId = NormalizedEntityId.from("test-entity")
  private[this] val shardId  = NormalizedShardId.from("test-shard")

  "Leader" should {

    "他のメンバーの古い Term の RequestVote には Deny する" in {
      val leader = createRaftActor()
      val term1  = Term.initial()
      val term2  = term1.next()
      setState(leader, Leader, createLeaderData(term2))

      val anotherMemberIndex = createUniqueMemberIndex()

      leader ! RequestVote(shardId, term1, anotherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteDenied(term2))
    }

    "他のメンバーの進んだ Term の RequestVote には Accept して Follower になる" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val term1 = Term.initial()
      setState(leader, Leader, createLeaderData(term1))

      val anotherMemberIndex = createUniqueMemberIndex()
      val term2              = term1.next()
      leader ! RequestVote(shardId, term2, anotherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term2, leaderMemberIndex))
      getState(leader).stateName should be(Follower)
    }

    "deny RequestVote if lastLogIndex is older than own even if the request has same lastLogTerm" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val candidateMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! RequestVote(shardId, term2, candidateMemberIndex, lastLogIndex = index1, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term1))
    }

    "deny RequestVote if lastLogTerm is older than own even if the request has newer lastLogIndex than own" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val candidateMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val term3                = Term(3)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term2),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term2, log))

      leader ! RequestVote(shardId, term3, candidateMemberIndex, lastLogIndex = index3, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term2))
    }

    "AppendEntries が古い Term を持っているときは拒否" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term1              = Term.initial()
      val term2              = term1.next()
      setState(leader, Leader, createLeaderData(term2))

      leader ! createAppendEntries(shardId, term1, anotherMemberIndex)
      expectMsg(AppendEntriesFailed(term2, leaderMemberIndex))
    }

    "AppendEntries が新しい Term を持っているときは Follower に降格" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term1              = Term.initial()
      val term2              = term1.next()
      setState(leader, Leader, createLeaderData(term1))

      leader ! createAppendEntries(shardId, term2, anotherMemberIndex)
      expectMsg(AppendEntriesSucceeded(term2, LogEntryIndex(0), leaderMemberIndex))

      getState(leader).stateName should be(Follower)
    }

    "コマンドを ReplicationActor に転送する" ignore {}

    "AppendEntries が新しい Term を持っていて、prevLogIndex/prevLogTerm に一致するログエントリがある場合は AppendEntriesSucceeded" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index2, term1, logEntries)
      expectMsg(AppendEntriesSucceeded(term2, index2, leaderMemberIndex))
    }

    "AppendEntries が新しい Term を持っていて、prevLogIndex/prevLogTerm に一致するログエントリがない場合は AppendEntriesFailed" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index3, term1, logEntries)
      expectMsg(AppendEntriesFailed(term2, leaderMemberIndex))
    }

    "prevLogIndex の Term が prevLogTerm に一致するログエントリでない場合は AppendEntriesFailed を返す" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val index4               = LogEntryIndex(4)
      val followerLogEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
        LogEntry(index3, EntityEvent(Option(entityId), "c"), term1),
      )
      val leaderLogEntries = Seq(
        LogEntry(index4, EntityEvent(Option(entityId), "e"), term1.next()),
      )
      val log = ReplicatedLog().merge(followerLogEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term1, newLeaderMemberIndex, index3, term1.next(), leaderLogEntries)
      expectMsg(AppendEntriesFailed(term1, leaderMemberIndex))
    }

    "leaderCommit > commitIndex となる場合、 commitIndex に min(leaderCommit, 新規エントリの最後のインデックス) を設定" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      // leaderCommit > commitIndex
      val index2       = LogEntryIndex(2)
      val leaderCommit = LogEntryIndex(3)
      val logEntries1 = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), term1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), term1),
      )
      val log        = ReplicatedLog().merge(logEntries1, LogEntryIndex.initial())
      val leaderData = createLeaderData(term1, log, index2)

      val logEntries2 = Seq(
        LogEntry(leaderCommit, EntityEvent(Option(entityId), "c"), term1),
      )
      setState(leader, Leader, leaderData)
      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index2, term1, logEntries2, leaderCommit)
      expectMsg(AppendEntriesSucceeded(term2, leaderCommit, leaderMemberIndex))

      getState(leader).stateData.commitIndex should be(leaderCommit)
    }

    "RequestVote の Term が新しくてもログが古い場合は否認する" in {
      val leader    = createRaftActor()
      val term1     = Term.initial()
      val leaderLog = ReplicatedLog().append(EntityEvent(Option(entityId), "dummy"), term1)
      setState(leader, Leader, createLeaderData(term1, log = leaderLog))

      val anotherMemberIndex = createUniqueMemberIndex()
      val term2              = term1.next()
      val lastLogIndex       = LogEntryIndex.initial()
      leader ! RequestVote(shardId, term2, anotherMemberIndex, lastLogIndex, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term1))
    }
  }

  private[this] def createLeaderData(
      currentTerm: Term,
      log: ReplicatedLog = ReplicatedLog(),
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      nextIndex: NextIndex = NextIndex(ReplicatedLog()),
      matchIndex: MatchIndex = MatchIndex(),
  ): RaftMemberData =
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = log,
      commitIndex = commitIndex,
      nextIndex = Some(nextIndex),
      matchIndex = matchIndex,
    ).initializeLeaderData()
}
