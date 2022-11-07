package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.RaftProtocol.{
  Command,
  ForwardedCommand,
  ProcessCommand,
  Replicate,
  ReplicationFailed,
}
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.Inside

class RaftActorCandidateSpec
    extends TestKit(ActorSystem("RaftActorCandidateSpec", RaftActorSpecBase.configWithPersistenceTestKits))
    with RaftActorSpecBase
    with Inside {

  import RaftActor._

  private[this] val shardId  = NormalizedShardId.from("test-shard")
  private[this] val entityId = NormalizedEntityId.from("test-entity")

  private implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped
  private val persistenceTestKit                                          = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
  }

  "Candidate" should {

    "send RequestVote(lastLogIndex=0, lastLogTerm=0) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm=0, ancestorLastIndex=0, entries.size=0, ...)" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val regionProbe          = TestProbe()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm   = Term(2)
      val candidateData = createCandidateData(currentTerm, ReplicatedLog())
      setState(candidate, Candidate, candidateData)

      assert(candidateData.replicatedLog.ancestorLastIndex == LogEntryIndex(0))
      assert(candidateData.replicatedLog.ancestorLastTerm == Term(0))
      assert(candidateData.replicatedLog.entries.sizeIs == 0)

      // ElectionTimeout triggers that this candidate sends a RequestVote.
      candidate ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = candidateMemberIndex,
          lastLogIndex = LogEntryIndex(0),
          lastLogTerm = Term(0),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(candidate).stateName == Candidate)
        val stateData = getState(candidate).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote(lastLogIndex=entries.last.index, lastLogTerm=entries.last.term) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm=0, ancestorLastIndex=0, entries.size>0, ...)" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val regionProbe          = TestProbe()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm = Term(2)
      val replicatedLog = {
        val candidateLogEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(candidateLogEntries)
      }
      val candidateData = createCandidateData(currentTerm, replicatedLog)
      setState(candidate, Candidate, candidateData)

      assert(candidateData.replicatedLog.ancestorLastIndex == LogEntryIndex(0))
      assert(candidateData.replicatedLog.ancestorLastTerm == Term(0))
      assert(candidateData.replicatedLog.entries.sizeIs > 0)

      // ElectionTimeout triggers that this candidate sends a RequestVote.
      candidate ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = candidateMemberIndex,
          lastLogIndex = LogEntryIndex(3),
          lastLogTerm = Term(2),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(candidate).stateName == Candidate)
        val stateData = getState(candidate).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote(lastLogIndex=ancestorLastIndex, lastLogTerm=ancestorLastTerm) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm>0, ancestorLastIndex>0, entries.size=0, ...)" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val regionProbe          = TestProbe()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm       = Term(2)
      val ancestorLastTerm  = Term(1)
      val ancestorLastIndex = LogEntryIndex(5)
      val candidateData = {
        val replicatedLog = ReplicatedLog().reset(ancestorLastTerm, ancestorLastIndex)
        createCandidateData(currentTerm, replicatedLog)
      }
      setState(candidate, Candidate, candidateData)

      assert(candidateData.replicatedLog.ancestorLastIndex == ancestorLastIndex)
      assert(candidateData.replicatedLog.ancestorLastTerm == ancestorLastTerm)
      assert(candidateData.replicatedLog.entries.sizeIs == 0)

      // ElectionTimeout triggers that this candidate sends a RequestVote.
      candidate ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = candidateMemberIndex,
          lastLogIndex = ancestorLastIndex,
          lastLogTerm = ancestorLastTerm,
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(candidate).stateName == Candidate)
        val stateData = getState(candidate).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote(lastLogIndex=entries.last.index, lastLogTerm=entries.last.term) on ElectionTimeout if it has RaftMemberData(ancestorLastTerm>0, ancestorLastIndex>0, entries.size>0, ...)" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val regionProbe          = TestProbe()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
        region = regionProbe.ref,
      )
      val currentTerm       = Term(2)
      val ancestorLastTerm  = Term(1)
      val ancestorLastIndex = LogEntryIndex(3)
      val candidateData = {
        val candidateLogEntries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "a"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "b"), Term(2)),
          LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "c"), Term(2)),
        )
        val replicatedLog = ReplicatedLog()
          .reset(ancestorLastTerm, ancestorLastIndex)
          .truncateAndAppend(candidateLogEntries)
        createCandidateData(currentTerm, replicatedLog)
      }
      setState(candidate, Candidate, candidateData)

      assert(candidateData.replicatedLog.ancestorLastIndex == ancestorLastIndex)
      assert(candidateData.replicatedLog.ancestorLastTerm == ancestorLastTerm)
      assert(candidateData.replicatedLog.entries.sizeIs > 0)

      // ElectionTimeout triggers that this candidate sends a RequestVote.
      candidate ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = candidateMemberIndex,
          lastLogIndex = LogEntryIndex(6),
          lastLogTerm = Term(2),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(candidate).stateName == Candidate)
        val stateData = getState(candidate).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "send RequestVote on ElectionTimeout if it is defined as sticky leader" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val regionProbe          = TestProbe()
      val configStickyLeader = ConfigFactory
        .parseString(s"""
        lerna.akka.entityreplication.raft.sticky-leaders = {
          "${shardId.raw}" = "${candidateMemberIndex.role}"
        }
        // electionTimeout がテスト中に自動で発生しないようにする
        lerna.akka.entityreplication.raft.election-timeout = 999999s""").withFallback(config)
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
        region = regionProbe.ref,
        settings = RaftSettings(configStickyLeader),
      )
      val currentTerm   = Term(2)
      val candidateData = createCandidateData(currentTerm, ReplicatedLog())
      setState(candidate, Candidate, candidateData)

      // ElectionTimeout triggers that this candidate sends a RequestVote.
      candidate ! ElectionTimeout
      val expectedRequestVote =
        RequestVote(
          shardId,
          term = Term(3),
          candidate = candidateMemberIndex,
          lastLogIndex = LogEntryIndex(0),
          lastLogTerm = Term(0),
        )
      regionProbe.expectMsg(ReplicationRegion.Broadcast(expectedRequestVote))
      awaitAssert {
        assert(getState(candidate).stateName == Candidate)
        val stateData = getState(candidate).stateData
        assert(stateData.currentTerm == Term(3))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "not send RequestVote on ElectionTimeout if other raft actors are defined as a sticky leader and it's not defined as a sticky leader" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val regionProbe          = TestProbe()
      val configStickyLeader = ConfigFactory
        .parseString(s"""
        lerna.akka.entityreplication.raft.sticky-leaders = {
          "other-actors-shard-id" = "other-actors-role"
        }
        // electionTimeout がテスト中に自動で発生しないようにする
        lerna.akka.entityreplication.raft.election-timeout = 999999s""").withFallback(config)
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
        region = regionProbe.ref,
        settings = RaftSettings(configStickyLeader),
      )
      val currentTerm   = Term(2)
      val candidateData = createCandidateData(currentTerm, ReplicatedLog())
      setState(candidate, Candidate, candidateData)

      // ElectionTimeout should not triggers that this candidate sends a RequestVote.
      candidate ! ElectionTimeout
      regionProbe.expectNoMessage()
      awaitAssert {
        assert(getState(candidate).stateName == Candidate)
        val stateData = getState(candidate).stateData
        assert(stateData.currentTerm == Term(2))
        assert(stateData.votedFor.isEmpty)
        assert(stateData.acceptedMembers.isEmpty)
      }
    }

    "メンバーの過半数に Accept されると Leader になる" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val follower1MemberIndex = createUniqueMemberIndex()
      val follower2MemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = selfMemberIndex,
        otherMemberIndexes = Set(follower1MemberIndex, follower2MemberIndex),
      )
      val currentTerm = Term(1)
      setState(candidate, Candidate, createCandidateData(currentTerm))
      inside(getState(candidate)) { state =>
        state.stateData.acceptedMembers should be(Set.empty)
      }

      // The candidate will vote for itself.
      candidate ! RequestVoteAccepted(currentTerm, selfMemberIndex)
      inside(getState(candidate)) { state =>
        state.stateName should be(Candidate)
        state.stateData.currentTerm should be(currentTerm)
        state.stateData.acceptedMembers should be(Set(selfMemberIndex))
      }

      // Denied from follower1, this situation could happen by a split vote.
      candidate ! RequestVoteDenied(currentTerm)
      inside(getState(candidate)) { state =>
        state.stateName should be(Candidate)
        state.stateData.currentTerm should be(currentTerm)
        state.stateData.acceptedMembers should be(Set(selfMemberIndex))
      }

      candidate ! RequestVoteAccepted(currentTerm, follower2MemberIndex)
      inside(getState(candidate)) { state =>
        state.stateName should be(Leader)
        state.stateData.currentTerm should be(currentTerm)
      }
    }

    "become a Follower and agree to a Term if it receives RequestVoteDenied with newer Term" in {
      val follower1MemberIndex = createUniqueMemberIndex()
      val follower2MemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        otherMemberIndexes = Set(follower1MemberIndex, follower2MemberIndex),
      )
      val selfTerm  = Term(1)
      val newerTerm = selfTerm.next()
      setState(candidate, Candidate, createCandidateData(selfTerm))

      candidate ! RequestVoteDenied(newerTerm)
      candidate ! RequestVoteAccepted(selfTerm, follower2MemberIndex)
      val state = getState(candidate)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(newerTerm)
      state.stateData.leaderMember should be(None)
    }

    "not become the leader by duplicated votes from the same follower" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val follower1MemberIndex = createUniqueMemberIndex()
      val follower2MemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = selfMemberIndex,
        otherMemberIndexes = Set(follower1MemberIndex, follower2MemberIndex),
      )
      val currentTerm = Term(1)
      setState(candidate, Candidate, createCandidateData(currentTerm))
      inside(getState(candidate)) { state =>
        state.stateData.acceptedMembers should be(Set.empty)
      }

      // For simplicity, this test skips that the candidate votes for itself.

      // The first vote from follower1
      candidate ! RequestVoteAccepted(currentTerm, follower1MemberIndex)
      inside(getState(candidate)) { state =>
        state.stateName should be(Candidate)
        state.stateData.currentTerm should be(currentTerm)
        state.stateData.acceptedMembers should be(Set(follower1MemberIndex))
      }

      // The second duplicated vote from follower1.
      // The candidate shouldn't become the leader
      // since it doesn't receive votes from the majority of the members.
      candidate ! RequestVoteAccepted(currentTerm, follower1MemberIndex)
      inside(getState(candidate)) { state =>
        state.stateName should be(Candidate)
        state.stateData.currentTerm should be(currentTerm)
        state.stateData.acceptedMembers should be(Set(follower1MemberIndex))
      }
    }

    "AppendEntries が古い Term を持っているときは拒否" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term1              = Term.initial()
      val term2              = term1.next()
      setState(candidate, Candidate, createCandidateData(term2))

      candidate ! createAppendEntries(shardId, term1, anotherMemberIndex)
      expectMsg(AppendEntriesFailed(term2, candidateMemberIndex))
    }

    "AppendEntries が新しい Term を持っているときは Follower に降格" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term1              = Term.initial()
      val term2              = term1.next()
      setState(candidate, Candidate, createCandidateData(term1))

      candidate ! createAppendEntries(shardId, term2, anotherMemberIndex)
      expectMsg(AppendEntriesSucceeded(term2, LogEntryIndex(0), candidateMemberIndex))

      val state = getState(candidate)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(term2)
      state.stateData.leaderMember should contain(anotherMemberIndex)
    }

    "AppendEntries が同じ Term を持っているときは（先に別のリーダーが当選したということなので）Follower に降格" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term               = Term.initial().next()
      setState(candidate, Candidate, createCandidateData(term))

      candidate ! createAppendEntries(shardId, term, anotherMemberIndex)
      expectMsg(AppendEntriesSucceeded(term, LogEntryIndex(0), candidateMemberIndex))

      val state = getState(candidate)
      state.stateName should be(Follower)
      state.stateData.leaderMember should contain(anotherMemberIndex)
    }

    "コマンドは保留し、フォロワーに降格した場合はリーダーに転送する" in {
      val region               = TestProbe()
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        region = region.ref,
        selfMemberIndex = candidateMemberIndex,
      )
      val term1 = Term.initial()
      val term2 = term1.next()
      setState(candidate, Candidate, createCandidateData(term1))

      case object SomeCommand
      // コマンドはこの時点では保留
      candidate ! Command(SomeCommand)

      val leaderMemberIndex = createUniqueMemberIndex()
      // term2 のメンバー を leader として認識させる
      candidate ! createAppendEntries(shardId, term2, leaderMemberIndex)
      expectMsg(AppendEntriesSucceeded(term2, LogEntryIndex(0), candidateMemberIndex))
      // コマンドが leader に転送される
      region.expectMsg(ReplicationRegion.DeliverTo(leaderMemberIndex, ForwardedCommand(Command(SomeCommand))))
    }

    "stash commands and forward it to ReplicationActor after it commits initial NoOp event as a leader" in {
      val region               = TestProbe()
      val replicationActor     = TestProbe()
      val candidateMemberIndex = createUniqueMemberIndex()
      val follower1MemberIndex = createUniqueMemberIndex()
      val follower2MemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
        otherMemberIndexes = Set(follower1MemberIndex, follower2MemberIndex),
        region = region.ref,
        replicationActor = replicationActor.ref,
      )
      val term = Term.initial()
      setState(candidate, Candidate, createCandidateData(term))

      case object SomeCommand
      // the command will be stashed because the member is not a leader yet
      candidate ! Command(SomeCommand)

      // become leader by election
      candidate ! RequestVoteAccepted(term, follower1MemberIndex)
      candidate ! RequestVoteAccepted(term, follower2MemberIndex)
      getState(candidate).stateName should be(Leader)
      val leader = candidate
      // commit initial NoOp event
      awaitCond(getState(leader).stateData.replicatedLog.lastOption.exists(e => e.event.event == NoOp))
      val lastLogIndex = getState(leader).stateData.replicatedLog.lastLogIndex
      leader ! AppendEntriesSucceeded(term, lastLogIndex, follower1MemberIndex)
      leader ! AppendEntriesSucceeded(term, lastLogIndex, follower2MemberIndex)

      // the leader activates the entity
      replicationActor.expectMsgType[RaftProtocol.Activate]
      // the leader forwards the command to ReplicationActor
      replicationActor.expectMsg(ProcessCommand(SomeCommand))
    }

    "AppendEntries の prevLogIndex/prevLogTerm に一致するログエントリがある場合は AppendEntriesSucceeded" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val term1             = Term(1)
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)

      // init candidate
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log           = ReplicatedLog().truncateAndAppend(logEntries)
      val candidateData = createCandidateData(term1, log, index2)

      // send appendEntries
      setState(candidate, Candidate, candidateData)
      candidate ! createAppendEntries(
        shardId,
        term1,
        leaderMemberIndex,
        prevLogIndex = index2,
        prevLogTerm = term1,
        leaderCommit = index2,
      )
      expectMsg(AppendEntriesSucceeded(term1, index2, candidateMemberIndex))
    }

    "AppendEntries の prevLogIndex/prevLogTerm に一致するログエントリがない場合は AppendEntriesFailed" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val term1             = Term(1)
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val index3            = LogEntryIndex(3)
      case object SomeEvent1
      case object SomeEvent2
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), SomeEvent1), term1),
        LogEntry(index2, EntityEvent(Option(entityId), SomeEvent2), term1),
      )

      val log           = ReplicatedLog().truncateAndAppend(logEntries)
      val candidateData = createCandidateData(term1, log, index2)

      setState(candidate, Candidate, candidateData)
      candidate ! createAppendEntries(shardId, term1, leaderMemberIndex, index3, term1)
      expectMsg(AppendEntriesFailed(term1, candidateMemberIndex))

      val state = getState(candidate)
      state.stateName should be(Follower)
      state.stateData.leaderMember should contain(leaderMemberIndex)
    }

    "prevLogIndex の Term が prevLogTerm に一致するログエントリでない場合は AppendEntriesFailed を返す" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val index3            = LogEntryIndex(3)
      val index4            = LogEntryIndex(4)
      val term              = Term(1)
      val followerLogEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term),
        LogEntry(index3, EntityEvent(Option(entityId), "c"), term),
      )
      val leaderLogEntries = Seq(
        LogEntry(index4, EntityEvent(Option(entityId), "e"), term.next()),
      )
      val log = ReplicatedLog().truncateAndAppend(followerLogEntries)
      setState(candidate, Candidate, createCandidateData(term, log))

      candidate ! createAppendEntries(shardId, term, leaderMemberIndex, index3, term.next(), leaderLogEntries)
      expectMsg(AppendEntriesFailed(Term(1), candidateMemberIndex))
    }

    "become a Follower and agree to a Term if it receives AppendEntries which includes log entries that cannot be merged and newer Term" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val index1            = LogEntryIndex(1)
      val index2            = LogEntryIndex(2)
      val index3            = LogEntryIndex(3)
      val index4            = LogEntryIndex(4)
      val selfTerm          = Term(1)
      val leaderTerm        = selfTerm.next()
      val candidateLogEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), selfTerm),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), selfTerm),
        LogEntry(index3, EntityEvent(Option(entityId), "c"), selfTerm),
      )
      val appendLogEntries = Seq(
        LogEntry(index4, EntityEvent(Option(entityId), "e"), leaderTerm),
      )
      val log = ReplicatedLog().truncateAndAppend(candidateLogEntries)
      setState(candidate, Candidate, createCandidateData(selfTerm, log))

      candidate ! createAppendEntries(shardId, leaderTerm, leaderMemberIndex, index3, leaderTerm, appendLogEntries)
      expectMsg(AppendEntriesFailed(leaderTerm, candidateMemberIndex))

      val state = getState(candidate)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(leaderTerm)
      state.stateData.leaderMember should contain(leaderMemberIndex)
    }

    "leaderCommit > commitIndex となる場合、 commitIndex に min(leaderCommit, 新規エントリの最後のインデックス) を設定" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val leaderMemberIndex = createUniqueMemberIndex()
      val term1             = Term(1)

      // leaderCommit > commitIndex
      val index1       = LogEntryIndex(1)
      val index2       = LogEntryIndex(2)
      val leaderCommit = LogEntryIndex(3)
      val logEntries1 = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log           = ReplicatedLog().truncateAndAppend(logEntries1)
      val candidateData = createCandidateData(term1, log, index2)

      val logEntries2 = Seq(
        LogEntry(leaderCommit, EntityEvent(Option(entityId), "c"), term1),
      )
      setState(candidate, Candidate, candidateData)
      candidate ! createAppendEntries(shardId, term1, leaderMemberIndex, index2, term1, logEntries2, leaderCommit)
      expectMsg(AppendEntriesSucceeded(term1, leaderCommit, candidateMemberIndex))

      getState(candidate).stateData.commitIndex should be(leaderCommit)
    }

    "update commitIndex to the last index of received entries (AppendEntries.entries.last.index) " +
    "if AppendEntries.leaderCommit is greater than commitIndex and the last index of received entries" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
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
      setState(candidate, Candidate, createCandidateData(Term(3), replicatedLog, commitIndex = LogEntryIndex(4)))

      val leaderMemberIndex = createUniqueMemberIndex()
      val leader            = TestProbe()

      candidate.tell(
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
      getState(candidate).stateData.commitIndex should be(LogEntryIndex(4))
    }

    "persist the whole new entries starting with the lastLogIndex + 1 " +
    "if the received AppendEntries message contains no existing entries" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
      )
      val candidatePersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = candidateMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(candidate, Candidate, createCandidateData(Term(3), replicatedLog))

      val leaderMemberIndex = createUniqueMemberIndex()
      candidate ! createAppendEntries(
        shardId,
        Term(3),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(3),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(5), candidateMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](candidatePersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(candidate)) { candidateState =>
        candidateState.stateName should be(Follower)
        inside(candidateState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "persist only new entries starting with the lastLogIndex + 1 " +
    "if the received AppendEntries message contains some existing entries" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
      )
      val candidatePersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = candidateMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(candidate, Candidate, createCandidateData(Term(3), replicatedLog))

      val leaderMemberIndex = createUniqueMemberIndex()
      candidate ! createAppendEntries(
        shardId,
        Term(3),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(2),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(5), candidateMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](candidatePersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(candidate)) { candidateState =>
        candidateState.stateName should be(Follower)
        inside(candidateState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "persist only new entries (beginning with the first conflict) " +
    "if the received AppendEntries message contains conflict entries" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
      )
      val candidatePersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = candidateMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(4)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(candidate, Candidate, createCandidateData(Term(5), replicatedLog, commitIndex = LogEntryIndex(2)))

      val leaderMemberIndex = createUniqueMemberIndex()
      candidate ! createAppendEntries(
        shardId,
        Term(5),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(2),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(5)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(5)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(5), LogEntryIndex(5), candidateMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](candidatePersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(5))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(5)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(5)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(candidate)) { candidateState =>
        candidateState.stateName should be(Follower)
        inside(candidateState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
              LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(5)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(5)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "truncate no entries even if the received AppendEntries message contains all existing entries (not including the last entry)" in {
      val shardId              = createUniqueShardId()
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        shardId = shardId,
        selfMemberIndex = candidateMemberIndex,
      )
      val candidatePersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = candidateMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(candidate, Candidate, createCandidateData(Term(3), replicatedLog))

      val leaderMemberIndex = createUniqueMemberIndex()
      candidate ! createAppendEntries(
        shardId,
        Term(3),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(2),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          // The following entries will be sent in another AppendEntries batch.
          // LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
          // LogEntry(LogEntryIndex(5), EntityEvent(None, NoOp), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(4), candidateMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](candidatePersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          appendedEntries.logEntries should be(empty)
      }
      inside(getState(candidate)) { candidateState =>
        candidateState.stateName should be(Follower)
        inside(candidateState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "reply with a ReplicationFailed message to a Replicate message and log a warning" in {
      val replicationActor = TestProbe()
      val entityId         = NormalizedEntityId("entity-1")
      val entityInstanceId = EntityInstanceId(1)

      val candidate     = createRaftActor()
      val candidateData = createCandidateData(Term(1))
      setState(candidate, Candidate, candidateData)

      LoggingTestKit
        .warn(
          "[Candidate] cannot replicate the event: type=[java.lang.String], entityId=[Some(entity-1)], instanceId=[Some(1)], entityLastAppliedIndex=[Some(2)]",
        ).expect {
          candidate ! Replicate(
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

  }

  private[this] def createCandidateData(
      currentTerm: Term,
      log: ReplicatedLog = ReplicatedLog(),
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      acceptedMembers: Set[MemberIndex] = Set(),
  ): RaftMemberData =
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = log,
      commitIndex = commitIndex,
      acceptedMembers = acceptedMembers,
    ).initializeCandidateData()
}
