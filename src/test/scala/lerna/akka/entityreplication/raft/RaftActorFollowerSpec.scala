package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._

class RaftActorFollowerSpec extends TestKit(ActorSystem()) with RaftActorSpecBase {

  import RaftActor._

  private[this] val entityId = NormalizedEntityId.from("test-entity")

  "Follower" should {

    "一度目の RequestVote には Accept する" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val candidateMemberIndex = createUniqueMemberIndex()
      val term                 = Term.initial().next()
      follower ! RequestVote(shardId, term, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term, followerMemberIndex))
    }

    "同じ Term の二度目の RequestVote には Deny する" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val candidateMemberIndex = createUniqueMemberIndex()
      val term                 = Term.initial().next()
      follower ! RequestVote(shardId, term, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term, followerMemberIndex))

      val anotherCandidateMemberIndex = createUniqueMemberIndex()

      follower ! RequestVote(shardId, term, anotherCandidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteDenied(term))
    }

    "既に Vote していても Term が進んでいれば Accept する" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val candidateMemberIndex = createUniqueMemberIndex()
      val term1                = Term.initial().next()
      follower ! RequestVote(shardId, term1, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term1, followerMemberIndex))

      val anotherCandidateMemberIndex = createUniqueMemberIndex()

      val term2 = term1.next()
      follower ! RequestVote(shardId, term2, anotherCandidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term2, followerMemberIndex))
    }

    "同じ Term の二度目の RequestVote でも、Candidate が同じなら Accept する" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val candidateMemberIndex = createUniqueMemberIndex()
      val term                 = Term.initial().next()
      follower ! RequestVote(shardId, term, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term, followerMemberIndex))

      follower ! RequestVote(shardId, term, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term, followerMemberIndex))
    }

    "deny RequestVote if lastLogIndex is older than own even if the request has same lastLogTerm" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
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
      setState(follower, Follower, createFollowerData(term1, log))

      follower ! RequestVote(shardId, term2, candidateMemberIndex, lastLogIndex = index1, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term2))
    }

    "deny RequestVote if lastLogTerm is older than own even if the request has newer lastLogIndex than own" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
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
      setState(follower, Follower, createFollowerData(term2, log))

      follower ! RequestVote(shardId, term3, candidateMemberIndex, lastLogIndex = index3, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term3))
    }

    "自分が持っている Term と同じ場合は AppendEntries を成功させる" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val candidateMemberIndex = createUniqueMemberIndex()
      val term                 = Term.initial().next()
      // term に同期させる
      follower ! RequestVote(shardId, term, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term, followerMemberIndex))

      val leaderMemberIndex = candidateMemberIndex
      follower ! createAppendEntries(shardId, term, leaderMemberIndex)
      expectMsg(AppendEntriesSucceeded(term, LogEntryIndex(0), followerMemberIndex))
    }

    "自分が持っている Term より新しい場合は AppendEntries を成功させる" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val candidateMemberIndex = createUniqueMemberIndex()
      val term1                = Term.initial().next()
      val term2                = term1.next()
      // term1 に同期させる
      follower ! RequestVote(shardId, term1, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term1, followerMemberIndex))

      val leaderMemberIndex = candidateMemberIndex
      follower ! createAppendEntries(shardId, term2, leaderMemberIndex)
      expectMsg(AppendEntriesSucceeded(term2, LogEntryIndex(0), followerMemberIndex))
    }

    "自分が持っている Term より古い場合は AppendEntries を失敗させる" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val otherMemberIndex = createUniqueMemberIndex()
      val term1            = Term.initial()
      val term2            = term1.next()
      // term2 に同期させる
      follower ! RequestVote(shardId, term2, otherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term2, followerMemberIndex))

      follower ! createAppendEntries(shardId, term1, otherMemberIndex)
      expectMsg(AppendEntriesFailed(term2, followerMemberIndex))
    }

    "リーダーにコマンドを転送する" in {
      val shardId             = createUniqueShardId()
      val region              = TestProbe()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        region = region.ref,
        selfMemberIndex = followerMemberIndex,
      )

      val term              = Term.initial()
      val leaderMemberIndex = createUniqueMemberIndex()

      // testActor を leader として認識させる
      follower ! createAppendEntries(shardId, term, leaderMemberIndex)
      expectMsg(AppendEntriesSucceeded(term, LogEntryIndex(0), followerMemberIndex))

      case object SomeCommand
      follower ! Command(SomeCommand)
      // コマンドが転送される
      region.expectMsg(ReplicationRegion.DeliverTo(leaderMemberIndex, ForwardedCommand(Command(SomeCommand))))
    }

    "リーダーが未確定のときは stash してリーダーが確定してからコマンドを転送する" in {
      val shardId             = createUniqueShardId()
      val region              = TestProbe()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        region = region.ref,
        selfMemberIndex = followerMemberIndex,
      )

      val term              = Term.initial().next()
      val leaderMemberIndex = createUniqueMemberIndex()

      case object SomeCommand
      follower ! Command(SomeCommand)

      // leader を認識させる
      follower ! createAppendEntries(shardId, term, leaderMemberIndex)
      expectMsg(AppendEntriesSucceeded(term, LogEntryIndex(0), followerMemberIndex))
      // コマンドが転送される
      region.expectMsg(ReplicationRegion.DeliverTo(leaderMemberIndex, ForwardedCommand(Command(SomeCommand))))
    }

    "leaderCommit > commitIndex となる場合、leaderCommit < 新規エントリの最後のインデックス なら、leaderCommit が commitIndex になる" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val index             = LogEntryIndex.initial()
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val index3            = LogEntryIndex(3)
      val term              = Term.initial()
      val term1             = Term(1)
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader            = TestProbe()
      val logEntries        = Seq(LogEntry(index1, EntityEvent(Option(entityId), "a"), term1))

      // leader を認識させる
      follower.tell(createAppendEntries(shardId, term, leaderMemberIndex), leader.ref)
      leader.expectMsg(AppendEntriesSucceeded(term, index, followerMemberIndex))

      follower.tell(
        createAppendEntries(
          shardId,
          term = term1,
          leader = leaderMemberIndex,
          prevLogIndex = LogEntryIndex(0),
          prevLogTerm = term,
          entries = logEntries,
          leaderCommit = index1,
        ),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(term1, index1, followerMemberIndex))

      // leaderCommit > commitIndex となる AppendEntries
      val newLogEntries = Seq(
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
        LogEntry(index3, EntityEvent(Option(entityId), "c"), term1),
      )
      // leaderCommit < 新規エントリ
      val leaderCommit = newLogEntries.head.index
      follower.tell(
        createAppendEntries(shardId, term1, leaderMemberIndex, index1, term1, newLogEntries, leaderCommit),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(term1, index3, followerMemberIndex))

      getState(follower).stateData.commitIndex should ===(index2)
    }

    "leaderCommit > commitIndex となる場合、leaderCommit = 新規エントリの最後のインデックス なら、両方と同じインデックス が commitIndex になる" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val index             = LogEntryIndex.initial()
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val term              = Term.initial()
      val term1             = Term(1)
      val leader            = TestProbe()
      val leaderMemberIndex = createUniqueMemberIndex()
      val logEntries        = Seq(LogEntry(index1, EntityEvent(Option(entityId), "a"), term1))

      // leader を認識させる
      follower.tell(createAppendEntries(shardId, term, leaderMemberIndex), leader.ref)
      leader.expectMsg(AppendEntriesSucceeded(term, index, followerMemberIndex))

      follower.tell(
        createAppendEntries(
          shardId,
          term = term1,
          leader = leaderMemberIndex,
          prevLogIndex = index,
          prevLogTerm = term1,
          entries = logEntries,
          leaderCommit = index1,
        ),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(term1, index1, followerMemberIndex))

      // leaderCommit > commitIndex となる AppendEntries
      val newLogEntries = Seq(
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      // leaderCommit = 新規エントリ
      val leaderCommit = newLogEntries.last.index
      follower.tell(
        createAppendEntries(shardId, term1, leaderMemberIndex, index1, term1, newLogEntries, leaderCommit),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(term1, leaderCommit, followerMemberIndex))

      getState(follower).stateData.commitIndex should ===(leaderCommit)
    }

    "leaderCommit > commitIndex となる場合、leaderCommit > 新規エントリの最後のインデックス なら、新規エントリの最後のインデックス が commitIndex になる" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val index             = LogEntryIndex.initial()
      val index1            = LogEntryIndex(1)
      val term              = Term.initial()
      val term1             = Term(1)
      val leader            = TestProbe()
      val leaderMemberIndex = createUniqueMemberIndex()
      val logEntries        = Seq(LogEntry(index1, EntityEvent(Option(entityId), "a"), term1))

      // leader を認識させる
      follower.tell(createAppendEntries(shardId, term, leaderMemberIndex), leader.ref)
      leader.expectMsg(AppendEntriesSucceeded(term, index, followerMemberIndex))

      follower.tell(
        createAppendEntries(
          shardId,
          term = term1,
          leader = leaderMemberIndex,
          prevLogIndex = index,
          prevLogTerm = term1,
          entries = logEntries,
          leaderCommit = index1,
        ),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(term1, index1, followerMemberIndex))

      // leaderCommit > commitIndex となる AppendEntries
      val lastIndex = LogEntryIndex(3)
      val newLogEntries = Seq(
        LogEntry(lastIndex, EntityEvent(Option(entityId), "c"), term1),
      )
      // leaderCommit > 新規エントリ
      val leaderCommit = LogEntryIndex(5)

      follower.tell(
        createAppendEntries(shardId, term1, leaderMemberIndex, index1, term1, newLogEntries, leaderCommit),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(term1, newLogEntries.last.index, followerMemberIndex))

      getState(follower).stateData.commitIndex should ===(newLogEntries.last.index)
    }

    "AppendEntries の prevLogIndex/prevLogTerm に一致するログエントリがある場合は AppendEntriesSucceeded" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val term1             = Term(1)
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log  = ReplicatedLog()
      val log1 = log.merge(logEntries, LogEntryIndex.initial())
      setState(follower, Follower, createFollowerData(term1, log1))

      follower ! createAppendEntries(shardId, term1, leaderMemberIndex, index2, term1)
      expectMsg(AppendEntriesSucceeded(term1, index2, followerMemberIndex))
    }

    "AppendEntries の prevLogIndex/prevLogTerm に一致するログエントリがない場合は AppendEntriesFailed" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val term1             = Term(1)
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(follower, Follower, createFollowerData(term1, log))

      follower ! createAppendEntries(
        shardId,
        term1,
        leaderMemberIndex,
        logEntries.last.index.next(),
        logEntries.last.term.next(),
        logEntries,
      )
      expectMsg(AppendEntriesFailed(term1, followerMemberIndex))
    }

    "leaderCommit > commitIndex となる場合、 commitIndex に min(leaderCommit, 新規エントリの最後のインデックス) を設定" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val term1             = Term(1)

      // leaderCommit > commitIndex
      val index2       = LogEntryIndex(2)
      val leaderCommit = LogEntryIndex(3)
      case object SomeEvent1
      case object SomeEvent2
      case object SomeEvent3
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), SomeEvent1), term1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), SomeEvent2), term1),
      )
      val log          = ReplicatedLog().merge(logEntries, index2)
      val followerData = createFollowerData(term1, log, index2)

      val newLogEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), SomeEvent1), term1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), SomeEvent2), term1),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), SomeEvent3), term1),
      )
      setState(follower, Follower, followerData)
      follower ! createAppendEntries(
        shardId,
        term1,
        leaderMemberIndex,
        index2,
        term1,
        newLogEntries,
        followerData.commitIndex.next(),
      )
      expectMsg(AppendEntriesSucceeded(term1, leaderCommit, followerMemberIndex))

      getState(follower).stateData.commitIndex should be(leaderCommit)
    }

    "prevLogIndex の Term が prevLogTerm に一致するログエントリでない場合は AppendEntriesFailed を返す" in {
      val shardId             = createUniqueShardId()
      val term1               = Term(1)
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val followerLogEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), Term(1)),
      )
      val log = ReplicatedLog().merge(followerLogEntries, LogEntryIndex.initial())
      setState(follower, Follower, createFollowerData(term1, log))

      follower ! createAppendEntries(
        shardId,
        term1,
        leaderMemberIndex,
        followerLogEntries.last.index,
        followerLogEntries.last.term.next(),
      )
      expectMsg(AppendEntriesFailed(term1, followerMemberIndex))
    }

    "Candidate のログよりも新しいログを持っている場合 RequestVote を否認する" in {
      val shardId             = createUniqueShardId()
      val term1               = Term.initial()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val followerData = getState(follower).stateData.asInstanceOf[FollowerData]
      // ログを一つ追加しておく
      val followerLog = ReplicatedLog().append(EntityEvent(Option(entityId), "dummy"), term1)
      setState(follower, Follower, followerData.asInstanceOf[RaftMemberDataImpl].copy(replicatedLog = followerLog))

      val otherMemberIndex = createUniqueMemberIndex()
      val term2            = term1.next()
      follower ! RequestVote(shardId, term2, otherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteDenied(term2))
    }
  }

  private[this] def createFollowerData(
      currentTerm: Term,
      log: ReplicatedLog,
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
  ): RaftMemberData =
    RaftMemberData(currentTerm = currentTerm, replicatedLog = log, commitIndex = commitIndex).initializeFollowerData()
}
