package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.RaftProtocol.{ Command, ForwardedCommand, ProcessCommand }
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.routing.MemberIndex

class RaftActorCandidateSpec extends TestKit(ActorSystem()) with RaftActorSpecBase {

  import RaftActor._

  private[this] val shardId  = NormalizedShardId.from("test-shard")
  private[this] val entityId = NormalizedEntityId.from("test-entity")

  "Candidate" should {

    "自分の RequestVote には Accept する" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate            = createRaftActor(selfMemberIndex = candidateMemberIndex)
      val term                 = Term.initial()
      setState(candidate, Candidate, createCandidateData(term))

      candidate ! RequestVote(shardId, term, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term, candidateMemberIndex))
    }

    "他のメンバーの同じ Term の RequestVote には Deny する" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate            = createRaftActor(selfMemberIndex = candidateMemberIndex)
      val term                 = Term.initial()
      setState(candidate, Candidate, createCandidateData(term))

      val anotherMemberIndex = createUniqueMemberIndex()
      candidate ! RequestVote(shardId, term, anotherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteDenied(term))
    }

    "他のメンバーの進んだ Term の RequestVote には Accept して Follower になる" in {
      val candidateMemberIndex = createUniqueMemberIndex()
      val candidate            = createRaftActor(selfMemberIndex = candidateMemberIndex)
      val term1                = Term.initial()
      setState(candidate, Candidate, createCandidateData(term1))

      val term2 = term1.next()
      candidate ! RequestVote(shardId, term2, candidateMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term2, candidateMemberIndex))
      getState(candidate).stateName should be(Follower)
    }

    "deny RequestVote if lastLogIndex is older than own even if the request has same lastLogTerm" in {
      val candidateMemberIndex1 = createUniqueMemberIndex()
      val candidate             = createRaftActor(selfMemberIndex = candidateMemberIndex1)

      val candidateMemberIndex2 = createUniqueMemberIndex()
      val term1                 = Term(1)
      val term2                 = Term(2)
      val index1                = LogEntryIndex(1)
      val index2                = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(candidate, Candidate, createCandidateData(term1, log))

      candidate ! RequestVote(shardId, term2, candidateMemberIndex2, lastLogIndex = index1, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term2))
    }

    "deny RequestVote if lastLogTerm is older than own even if the request has newer lastLogIndex than own" in {
      val candidateMemberIndex1 = createUniqueMemberIndex()
      val candidate             = createRaftActor(selfMemberIndex = candidateMemberIndex1)

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
      setState(candidate, Candidate, createCandidateData(term2, log))

      candidate ! RequestVote(shardId, term3, candidateMemberIndex, lastLogIndex = index3, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term3))
    }

    "メンバーの過半数に Accept されると Leader になる" in {
      val follower1MemberIndex = createUniqueMemberIndex()
      val follower2MemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        otherMemberIndexes = Set(follower1MemberIndex, follower2MemberIndex),
      )
      val term = Term.initial()
      setState(candidate, Candidate, createCandidateData(term))

      candidate ! RequestVoteAccepted(term, follower1MemberIndex)
      candidate ! RequestVoteAccepted(term, follower2MemberIndex)
      getState(candidate).stateName should be(Leader)
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

      getState(candidate).stateName should be(Follower)
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

      getState(candidate).stateName should be(Follower)
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
      val log           = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
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

      val log           = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      val candidateData = createCandidateData(term1, log, index2)

      setState(candidate, Candidate, candidateData)
      candidate ! createAppendEntries(shardId, term1, leaderMemberIndex, index3, term1)
      expectMsg(AppendEntriesFailed(term1, candidateMemberIndex))
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
      val log = ReplicatedLog().merge(followerLogEntries, LogEntryIndex.initial())
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
      val log = ReplicatedLog().merge(candidateLogEntries, LogEntryIndex.initial())
      setState(candidate, Candidate, createCandidateData(selfTerm, log))

      candidate ! createAppendEntries(shardId, leaderTerm, leaderMemberIndex, index3, leaderTerm, appendLogEntries)
      expectMsg(AppendEntriesFailed(leaderTerm, candidateMemberIndex))

      val state = getState(candidate)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(leaderTerm)
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
      val log           = ReplicatedLog().merge(logEntries1, LogEntryIndex.initial())
      val candidateData = createCandidateData(term1, log, index2)

      val logEntries2 = Seq(
        LogEntry(leaderCommit, EntityEvent(Option(entityId), "c"), term1),
      )
      setState(candidate, Candidate, candidateData)
      candidate ! createAppendEntries(shardId, term1, leaderMemberIndex, index2, term1, logEntries2, leaderCommit)
      expectMsg(AppendEntriesSucceeded(term1, leaderCommit, candidateMemberIndex))

      getState(candidate).stateData.commitIndex should be(leaderCommit)
    }

    "RequestVote の Term が新しくてもログが古い場合は否認する" in {
      val candidateMemberIndex      = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()
      val candidate = createRaftActor(
        selfMemberIndex = candidateMemberIndex,
      )
      val term1        = Term.initial()
      val candidateLog = ReplicatedLog().append(EntityEvent(Option(entityId), "dummy"), term1)
      setState(candidate, Candidate, createCandidateData(term1, log = candidateLog))

      val term2        = term1.next()
      val lastLogIndex = LogEntryIndex.initial()
      candidate ! RequestVote(shardId, term2, otherCandidateMemberIndex, lastLogIndex, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term2))
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
