package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId }
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.testkit.KryoSerializable
import org.scalatest.Inside

class RaftActorFollowerSpec
    extends TestKit(ActorSystem("RaftActorFollowerSpec", RaftActorSpecBase.configWithPersistenceTestKits))
    with RaftActorSpecBase
    with Inside {

  import RaftActor._

  private[this] val entityId = NormalizedEntityId.from("test-entity")

  private implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped
  private val persistenceTestKit                                          = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
  }

  "Follower" should {

    "send RequestVote(lastLogIndex=0, lastLogTerm=0) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm=0, ancestorLastIndex=0, entries.size=0, ...)" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm  = Term(2)
      val followerData = createFollowerData(currentTerm, ReplicatedLog())
      setState(follower, Follower, followerData)

      assert(followerData.replicatedLog.ancestorLastIndex == LogEntryIndex(0))
      assert(followerData.replicatedLog.ancestorLastTerm == Term(0))
      assert(followerData.replicatedLog.entries.sizeIs == 0)

      // ElectionTimeout triggers that this follower sends a RequestVote.
      follower ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = followerMemberIndex,
          lastLogIndex = LogEntryIndex(0),
          lastLogTerm = Term(0),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(follower).stateName == Candidate)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote(lastLogIndex=entries.last.index, lastLogTerm=entries.last.term) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm=0, ancestorLastIndex=0, entries.size>0, ...)" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm = Term(2)
      val replicatedLog = {
        val followerLogEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(followerLogEntries)
      }
      val followerData = createFollowerData(currentTerm, replicatedLog)
      setState(follower, Follower, followerData)

      assert(followerData.replicatedLog.ancestorLastIndex == LogEntryIndex(0))
      assert(followerData.replicatedLog.ancestorLastTerm == Term(0))
      assert(followerData.replicatedLog.entries.sizeIs > 0)

      // ElectionTimeout triggers that this follower sends a RequestVote.
      follower ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = followerMemberIndex,
          lastLogIndex = LogEntryIndex(3),
          lastLogTerm = Term(2),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(follower).stateName == Candidate)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote(lastLogIndex=ancestorLastIndex, lastLogTerm=ancestorLastTerm) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm>0, ancestorLastIndex>0, entries.size=0, ...)" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm       = Term(2)
      val ancestorLastTerm  = Term(1)
      val ancestorLastIndex = LogEntryIndex(5)
      val followerData = {
        val replicatedLog = ReplicatedLog().reset(ancestorLastTerm, ancestorLastIndex)
        createFollowerData(currentTerm, replicatedLog)
      }
      setState(follower, Follower, followerData)

      assert(followerData.replicatedLog.ancestorLastIndex == ancestorLastIndex)
      assert(followerData.replicatedLog.ancestorLastTerm == ancestorLastTerm)
      assert(followerData.replicatedLog.entries.sizeIs == 0)

      // ElectionTimeout triggers that this follower sends a RequestVote.
      follower ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = followerMemberIndex,
          lastLogIndex = ancestorLastIndex,
          lastLogTerm = ancestorLastTerm,
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(follower).stateName == Candidate)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote(lastLogIndex=entries.last.index, lastLogTerm=entries.last.term) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm>0, ancestorLastIndex>0, entries.size>0, ...)" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm       = Term(2)
      val ancestorLastTerm  = Term(1)
      val ancestorLastIndex = LogEntryIndex(3)
      val followerData = {
        val followerLogEntries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "a"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "b"), Term(2)),
          LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "c"), Term(2)),
        )
        val replicatedLog = ReplicatedLog()
          .reset(ancestorLastTerm, ancestorLastIndex)
          .truncateAndAppend(followerLogEntries)
        createFollowerData(currentTerm, replicatedLog)
      }
      setState(follower, Follower, followerData)

      assert(followerData.replicatedLog.ancestorLastIndex == ancestorLastIndex)
      assert(followerData.replicatedLog.ancestorLastTerm == ancestorLastTerm)
      assert(followerData.replicatedLog.entries.sizeIs > 0)

      // ElectionTimeout triggers that this follower sends a RequestVote.
      follower ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = followerMemberIndex,
          lastLogIndex = LogEntryIndex(6),
          lastLogTerm = Term(2),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(follower).stateName == Candidate)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote on ElectionTimeout if a shard which it's belongs to doesn't have a sticky leader" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val customSettings      = RaftSettings(defaultRaftConfig).withStickyLeaders(Map.empty)
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
        settings = customSettings,
      )
      val currentTerm  = Term(2)
      val followerData = createFollowerData(currentTerm, ReplicatedLog())
      setState(follower, Follower, followerData)

      // ElectionTimeout triggers that this follower sends a RequestVote.
      follower ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = followerMemberIndex,
          lastLogIndex = LogEntryIndex(0),
          lastLogTerm = Term(0),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(follower).stateName == Candidate)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote on ElectionTimeout if it is defined as sticky leader" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val customSettings =
        RaftSettings(defaultRaftConfig).withStickyLeaders(Map(shardId.raw -> followerMemberIndex.role))
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
        settings = customSettings,
      )
      val currentTerm  = Term(2)
      val followerData = createFollowerData(currentTerm, ReplicatedLog())
      setState(follower, Follower, followerData)

      // ElectionTimeout triggers that this follower sends a RequestVote.
      follower ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = followerMemberIndex,
          lastLogIndex = LogEntryIndex(0),
          lastLogTerm = Term(0),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(follower).stateName == Candidate)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "not send RequestVote on ElectionTimeout if another raft actor which belongs same shard is defined as a sticky leader" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val regionProbe         = TestProbe()
      val customSettings =
        RaftSettings(defaultRaftConfig).withStickyLeaders(Map(s"${shardId.raw}" -> "other-actors-role"))
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        region = regionProbe.ref,
        settings = customSettings,
      )
      val currentTerm  = Term(2)
      val followerData = createFollowerData(currentTerm, ReplicatedLog())
      setState(follower, Follower, followerData)

      // ElectionTimeout should not trigger that this follower sends a RequestVote.
      follower ! ElectionTimeout
      regionProbe.expectNoMessage()
      awaitAssert {
        assert(getState(follower).stateName == Follower)
        val stateData = getState(follower).stateData
        assert(stateData.currentTerm == Term(2))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
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

      val state = getState(follower)
      state.stateName should be(Follower)
      state.stateData.leaderMember should contain(leaderMemberIndex)
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

      val state = getState(follower)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(term2)
      state.stateData.leaderMember should contain(leaderMemberIndex)
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
      val leader            = TestProbe()
      val leaderMemberIndex = createUniqueMemberIndex()
      // leader を認識させる
      follower.tell(
        createAppendEntries(
          shardId,
          term = Term(1),
          leader = leaderMemberIndex,
          prevLogIndex = LogEntryIndex(0),
          prevLogTerm = Term(0),
          entries = Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
          leaderCommit = LogEntryIndex(0),
        ),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(Term(1), LogEntryIndex(1), followerMemberIndex))

      // leaderCommit > commitIndex となる AppendEntries
      val newLogEntries = Seq(
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "c"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "c"), Term(1)),
      )
      // leaderCommit > 新規エントリ
      val leaderCommit = LogEntryIndex(5)

      follower.tell(
        createAppendEntries(
          shardId,
          term = Term(1),
          leader = leaderMemberIndex,
          prevLogIndex = LogEntryIndex(1),
          prevLogTerm = Term(1),
          newLogEntries,
          leaderCommit,
        ),
        leader.ref,
      )
      leader.expectMsg(AppendEntriesSucceeded(Term(1), newLogEntries.last.index, followerMemberIndex))

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
      val log1 = log.truncateAndAppend(logEntries)
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
      val log = ReplicatedLog().truncateAndAppend(logEntries)
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

      val state = getState(follower)
      state.stateName should be(Follower)
      state.stateData.leaderMember should contain(leaderMemberIndex)
    }

    "agree to a term if it receives AppendEntries which includes log entries that cannot be merged and newer Term" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val index3            = LogEntryIndex(3)
      val index4            = LogEntryIndex(4)
      val selfTerm          = Term(1)
      val leaderTerm        = selfTerm.next()
      val followerLogEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), selfTerm),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), selfTerm),
        LogEntry(index3, EntityEvent(Option(entityId), "c"), selfTerm),
      )
      val appendLogEntries = Seq(
        LogEntry(index4, EntityEvent(Option(entityId), "e"), leaderTerm),
      )
      val log = ReplicatedLog().truncateAndAppend(followerLogEntries)
      setState(follower, Follower, createFollowerData(selfTerm, log))

      follower ! createAppendEntries(shardId, leaderTerm, leaderMemberIndex, index3, leaderTerm, appendLogEntries)
      expectMsg(AppendEntriesFailed(leaderTerm, followerMemberIndex))

      val state = getState(follower)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(leaderTerm)
      state.stateData.leaderMember should contain(leaderMemberIndex)
    }

    "leaderCommit > commitIndex となる場合、 commitIndex に min(leaderCommit, 新規エントリの最後のインデックス) を設定" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()

      case object SomeEvent1 extends KryoSerializable
      case object SomeEvent2 extends KryoSerializable
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), SomeEvent1), Term(1)),
      )
      val log          = ReplicatedLog().truncateAndAppend(logEntries)
      val followerData = createFollowerData(Term(1), log, LogEntryIndex(2))

      val newLogEntries = Seq(
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), SomeEvent2), Term(1)),
      )
      val leaderCommit = LogEntryIndex(3)
      assert(leaderCommit > followerData.commitIndex)
      assert(newLogEntries.exists(_.index == leaderCommit))

      setState(follower, Follower, followerData)
      follower ! createAppendEntries(
        shardId,
        Term(1),
        leaderMemberIndex,
        LogEntryIndex(2),
        Term(1),
        newLogEntries,
        leaderCommit,
      )
      expectMsg(AppendEntriesSucceeded(Term(1), LogEntryIndex(3), followerMemberIndex))

      getState(follower).stateData.commitIndex should be(leaderCommit)
    }

    "update commitIndex to the last index of received entries (AppendEntries.entries.last.index) " +
    "if AppendEntries.leaderCommit is greater than commitIndex and the last index of received entries" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val replicatedLog =
        ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
          ),
        )
      setState(follower, Follower, createFollowerData(Term(2), replicatedLog, commitIndex = LogEntryIndex(4)))

      val leaderMemberIndex = createUniqueMemberIndex()
      val leader            = TestProbe()

      follower.tell(
        createAppendEntries(
          shardId,
          Term(3),
          leaderMemberIndex,
          prevLogIndex = LogEntryIndex(3),
          prevLogTerm = Term(2),
          entries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
            // The following entries will be sent in another AppendEntries batch and will be in conflict.
            // LogEntry(LogEntryIndex(5), EntityEvent(None, NoOp), Term(3)),
          ),
          leaderCommit = LogEntryIndex(5),
        ),
        leader.ref,
      )

      leader.expectMsgType[AppendEntriesSucceeded]
      getState(follower).stateData.commitIndex should be(LogEntryIndex(4))
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
      val log = ReplicatedLog().truncateAndAppend(followerLogEntries)
      setState(follower, Follower, createFollowerData(term1, log))

      follower ! createAppendEntries(
        shardId,
        term1,
        leaderMemberIndex,
        followerLogEntries.last.index,
        followerLogEntries.last.term.next(),
      )
      expectMsg(AppendEntriesFailed(term1, followerMemberIndex))

      val state = getState(follower)
      state.stateName should be(Follower)
      state.stateData.leaderMember should contain(leaderMemberIndex)
    }

    "persist the whole new entries starting with the lastLogIndex + 1 " +
    "if the received AppendEntries message contains no existing entries" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val followerPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = followerMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(follower, Follower, createFollowerData(Term(2), replicatedLog))

      val leaderMemberIndex = createUniqueMemberIndex()
      follower ! createAppendEntries(
        shardId,
        Term(2),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(3),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(2), LogEntryIndex(5), followerMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](followerPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(2))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(follower)) { followerState =>
        followerState.stateName should be(Follower)
        inside(followerState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "persist only new entries starting with the lastLogIndex + 1 " +
    "if the received AppendEntries message contains some existing entries" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val followerPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = followerMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(follower, Follower, createFollowerData(Term(2), replicatedLog))

      val leaderMemberIndex = createUniqueMemberIndex()
      follower ! createAppendEntries(
        shardId,
        Term(2),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(2),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(2), LogEntryIndex(5), followerMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](followerPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(2))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(follower)) { followerState =>
        followerState.stateName should be(Follower)
        inside(followerState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
            )
            logEntries should be(expectedEntries)
            logEntries.map(_.event) should be(expectedEntries.map(_.event))
        }
      }
    }

    "persist only new entries (beginning with the first conflict) " +
    "if the received AppendEntries message contains conflict entries" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val followerPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = followerMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(follower, Follower, createFollowerData(Term(2), replicatedLog, commitIndex = LogEntryIndex(2)))

      val leaderMemberIndex = createUniqueMemberIndex()
      follower ! createAppendEntries(
        shardId,
        Term(3),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(2),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(3)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(5), followerMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](followerPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(3)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(follower)) { followerState =>
        followerState.stateName should be(Follower)
        inside(followerState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
              LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(3)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(3)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "truncate no entries even if the received AppendEntries message contains all existing entries (not including the last entry)" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val followerPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = followerMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(follower, Follower, createFollowerData(Term(2), replicatedLog))

      val leaderMemberIndex = createUniqueMemberIndex()
      follower ! createAppendEntries(
        shardId,
        Term(2),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(3),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
          // The following entries will be sent in another AppendEntries batch.
          // LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(2), LogEntryIndex(5), followerMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](followerPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(2))
          appendedEntries.logEntries should be(empty)
      }
      inside(getState(follower)) { followerState =>
        followerState.stateName should be(Follower)
        inside(followerState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-3"), Term(2)),
            )
            logEntries should be(expectedEntries)
            logEntries.map(_.event) should be(expectedEntries.map(_.event))
        }
      }
    }

    "reply with a ReplicationFailed message to a Replicate message and log a warning" in {
      val replicationActor = TestProbe()
      val entityId         = NormalizedEntityId("entity-1")
      val entityInstanceId = EntityInstanceId(1)

      val follower     = createRaftActor()
      val followerData = createFollowerData(Term(1), ReplicatedLog())
      setState(follower, Follower, followerData)

      LoggingTestKit
        .warn(
          "[Follower] cannot replicate the event: type=[java.lang.String], entityId=[Some(entity-1)], instanceId=[Some(1)], entityLastAppliedIndex=[Some(2)]",
        ).expect {
          follower ! Replicate(
            event = "event-1",
            replicationActor.ref,
            entityId,
            entityInstanceId,
            entityLastAppliedIndex = LogEntryIndex(2),
            originSender = system.deadLetters,
          )
          replicationActor.expectMsg(ReplicationFailed)
        }
    }

    "send a EntityPassivationPermitRequest message if it receives a Passivate message" in {
      val shardId           = createUniqueShardId()
      val leaderMemberIndex = createUniqueMemberIndex()
      val region            = TestProbe()
      val follower = {
        val raftActor =
          createRaftActor(shardId = shardId, region = region.ref, otherMemberIndexes = Set(leaderMemberIndex))
        val followerData = createFollowerData(Term(1), ReplicatedLog(), leaderMember = Some(leaderMemberIndex))
        setState(raftActor, Follower, followerData)
        raftActor
      }

      val passivationTargetEntityPath = follower.path / "passivation-target-entity-id"
      follower ! ReplicationRegion.Passivate(passivationTargetEntityPath, "stop message")

      val entityPassivationPermitRequest = region.fishForSpecificMessage[EntityPassivationPermitRequest]() {
        case ReplicationRegion.DeliverTo(`leaderMemberIndex`, message: EntityPassivationPermitRequest) => message
      }
      entityPassivationPermitRequest.shardId should be(shardId)
      entityPassivationPermitRequest.entityId should be(NormalizedEntityId("passivation-target-entity-id"))
      entityPassivationPermitRequest.stopMessage should be("stop message")
    }

    "send no EntityPassivationPermitRequest message if it receives a Passivate message but has no leader" in {
      val shardId = createUniqueShardId()

      val region = TestProbe()
      // Arrange: the region only receives EntityPassivationPermitRequest for simplicity.
      region.ignoreMsg {
        case ReplicationRegion.DeliverTo(_, _: EntityPassivationPermitRequest) => false
        case _                                                                 => true
      }

      val follower = {
        val raftActor = createRaftActor(shardId = shardId, region = region.ref)
        setState(raftActor, Follower, createFollowerData(Term(1), ReplicatedLog()))
        raftActor
      }
      getState(follower).stateData.leaderMember should be(None)

      val passivationTargetEntityPath = follower.path / "passivation-target-entity-id"
      follower ! ReplicationRegion.Passivate(passivationTargetEntityPath, "stop message")

      region.expectNoMessage()
    }

    "send no reply if it receives an EntityPassivationPermitRequest message" in {
      val shardId = createUniqueShardId()
      val follower = {
        val raftActor = createRaftActor(shardId = shardId)
        setState(raftActor, Follower, createFollowerData(Term(1), ReplicatedLog()))
        raftActor
      }

      val probe = TestProbe()
      // Arrange: the probe only receives EntityPassivationPermitResponse for simplicity.
      probe.ignoreMsg {
        case _: EntityPassivationPermitResponse => false
        case _                                  => true
      }

      val passivationTargetEntityId = NormalizedEntityId("passivation-target-entity-id")
      follower.tell(EntityPassivationPermitRequest(shardId, passivationTargetEntityId, "stop message"), probe.ref)

      probe.expectNoMessage()
    }

    "send a StopMessage to the passivation target entity if it receives an EntityPassivationPermitted message" in {
      val shardId = createUniqueShardId()
      val entity  = TestProbe()
      val follower = {
        val raftActor = createRaftActor(shardId = shardId, replicationActor = entity.ref)
        setState(raftActor, Follower, createFollowerData(Term(1), ReplicatedLog()))
        raftActor
      }
      val passivationTargetEntityId = NormalizedEntityId("passivation-target-entity-id")

      // Arrange: the entity only receives stop messages for simplicity.
      entity.ignoreMsg {
        case msg if msg != "stop message" => true
      }
      // Arrange: the passivation target entity starts.
      follower ! TryCreateEntity(shardId, passivationTargetEntityId)

      follower ! EntityPassivationPermitted(passivationTargetEntityId, "stop message")

      entity.expectMsg("stop message")
    }

    "send no message to the passivation target entity if it receives an EntityPassivationDenied message" in {
      val shardId = createUniqueShardId()
      val entity  = TestProbe()
      val follower = {
        val raftActor = createRaftActor(shardId = shardId, replicationActor = entity.ref)
        setState(raftActor, Follower, createFollowerData(Term(1), ReplicatedLog()))
        raftActor
      }
      val passivationTargetEntityId = NormalizedEntityId("passivation-target-entity-id")

      // Arrange: the entity ignores protocol messages for simplicity.
      entity.ignoreMsg {
        case _: Activate => true
      }
      // Arrange: the passivation target entity starts.
      follower ! TryCreateEntity(shardId, passivationTargetEntityId)

      follower ! EntityPassivationDenied(passivationTargetEntityId)

      entity.expectNoMessage()
    }

  }

  private[this] def createFollowerData(
      currentTerm: Term,
      log: ReplicatedLog,
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      leaderMember: Option[MemberIndex] = None,
  ): RaftMemberData =
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = log,
      commitIndex = commitIndex,
      leaderMember = leaderMember,
    ).initializeFollowerData()
}
