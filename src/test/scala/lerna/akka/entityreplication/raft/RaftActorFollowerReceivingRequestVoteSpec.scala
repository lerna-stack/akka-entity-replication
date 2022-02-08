package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, ReplicatedLog, Term }
import lerna.akka.entityreplication.raft.protocol.RaftCommands.{
  RequestVote,
  RequestVoteAccepted,
  RequestVoteDenied,
  RequestVoteResponse,
}
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Assertion, Inside }

final class RaftActorFollowerReceivingRequestVoteSpec
    extends TestKit(ActorSystem())
    with RaftActorSpecBase
    with Inside {

  import RaftActor._
  import RaftActorFollowerReceivingRequestVoteSpec._

  private val shardId = createUniqueShardId()

  private def verifyReceivingRequestVote(
      selfMemberIndex: MemberIndex,
      followerData: RaftMemberData,
      requestVote: RequestVote,
      expectedReplyMessage: RequestVoteResponse,
      verifyState: RaftTestProbe.RaftState => Assertion,
  ): Assertion = {
    val follower = createRaftActor(
      shardId = shardId,
      selfMemberIndex = selfMemberIndex,
    )
    setState(follower, Follower, followerData)
    follower ! requestVote
    expectMsg(expectedReplyMessage)
    verifyState(getState(follower))
  }

  private def expectFollowerState(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
      leaderMember: Option[MemberIndex],
  )(
      actual: RaftTestProbe.RaftState,
  ): Assertion = {
    assert(actual.stateName === Follower)
    assert(actual.stateData.currentTerm === currentTerm)
    assert(actual.stateData.votedFor === votedFor)
    assert(actual.stateData.leaderMember === leaderMember)
  }

  "Follower" should {

    "一度目の RequestVote には Accept する" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData         = createFollowerData(Term(0), votedFor = None, leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), candidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "同じ Term の二度目の RequestVote には Deny する" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), otherCandidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteDenied(Term(1))
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "既に Vote していても Term が進んでいれば Accept する" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(2), otherCandidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(otherCandidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "同じ Term の二度目の RequestVote でも、Candidate が同じなら Accept する" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), votedMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote if the term is older than own" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(4),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(2), candidateMemberIndex, lastLogIndex = LogEntryIndex(2), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(4))
      val verifyState          = expectFollowerState(Term(4), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote from a Candidate which has the same term if lastLogIndex is older than own even if the request has the same lastLogTerm" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(1),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(1), candidateMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(1))
      val verifyState          = expectFollowerState(Term(1), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote from a Candidate which has the newer term if lastLogIndex is older than own even if the request has the same lastLogTerm" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(1),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(2), candidateMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote if lastLogTerm is older than own even if the request has newer lastLogIndex than own" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(2),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(3), candidateMemberIndex, lastLogIndex = LogEntryIndex(3), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "Candidate のログよりも新しいログを持っている場合 RequestVote を否認する" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(1),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "dummy"), Term(1)),
          ),
        )
      }
      val requestVote          = RequestVote(shardId, Term(2), candidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

  }

}

object RaftActorFollowerReceivingRequestVoteSpec {

  private def createFollowerData(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
      leaderMember: Option[MemberIndex],
      log: ReplicatedLog,
  ): RaftMemberData = {
    RaftMemberData(
      currentTerm = currentTerm,
      votedFor = votedFor,
      replicatedLog = log,
      leaderMember = leaderMember,
    ).initializeFollowerData()
  }

  private def newReplicatedLog(
      entries: LogEntry*,
  ) = {
    ReplicatedLog()
      .reset(ancestorLastTerm = Term(0), ancestorLastIndex = LogEntryIndex(0))
      .merge(entries, LogEntryIndex(0))
  }

}
