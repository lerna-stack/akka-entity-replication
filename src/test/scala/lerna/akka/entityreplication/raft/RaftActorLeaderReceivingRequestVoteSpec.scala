package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands.{
  RequestVote,
  RequestVoteAccepted,
  RequestVoteDenied,
  RequestVoteResponse,
}
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Assertion, Inside }

final class RaftActorLeaderReceivingRequestVoteSpec extends TestKit(ActorSystem()) with RaftActorSpecBase with Inside {

  import RaftActor._
  import RaftActorLeaderReceivingRequestVoteSpec._

  private val shardId = createUniqueShardId()

  private def verifyReceivingRequestVote(
      selfMemberIndex: MemberIndex,
      leaderData: RaftMemberData,
      requestVote: RequestVote,
      expectedReplyMessage: RequestVoteResponse,
      verifyState: RaftTestProbe.RaftState => Assertion,
  ): Assertion = {
    require(leaderData.votedFor === Some(selfMemberIndex), "The leader should always vote for itself")
    val leader = createRaftActor(
      shardId = shardId,
      selfMemberIndex = selfMemberIndex,
    )
    setState(leader, Leader, leaderData)
    leader ! requestVote
    expectMsg(expectedReplyMessage)
    verifyState(getState(leader))
  }

  private def expectLeaderState(
      selfMemberIndex: MemberIndex,
      currentTerm: Term,
  )(actual: RaftTestProbe.RaftState): Assertion = {
    assert(actual.stateName === Leader)
    assert(actual.stateData.currentTerm === currentTerm)
    assert(actual.stateData.votedFor === Some(selfMemberIndex), "The leader should always vote for itself.")
  }

  private def expectFollowerState(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
  )(
      actual: RaftTestProbe.RaftState,
  ): Assertion = {
    assert(actual.stateName === Follower)
    assert(actual.stateData.currentTerm === currentTerm)
    assert(actual.stateData.votedFor === votedFor)
    assert(
      actual.stateData.leaderMember === None,
      "A follower should not know the leader immediately after it comes from the leader.",
    )
  }

  "Leader" should {

    "deny RequestVote(term < currentTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val leaderData = createLeaderData(selfMemberIndex, Term(2), newReplicatedLog())
      val requestVote =
        RequestVote(shardId, Term(1), candidateMemberIndex, lastLogIndex = LogEntryIndex(0), lastLogTerm = Term(0))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectLeaderState(selfMemberIndex, Term(2))(_)

      verifyReceivingRequestVote(selfMemberIndex, leaderData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, lastLogIndex = log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val leaderData = createLeaderData(selfMemberIndex, Term(1), newReplicatedLog())
      val requestVote =
        RequestVote(shardId, Term(2), candidateMemberIndex, lastLogIndex = LogEntryIndex(0), lastLogTerm = Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState =
        expectFollowerState(Term(2), votedFor = Some(candidateMemberIndex))(_)

      verifyReceivingRequestVote(selfMemberIndex, leaderData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val leaderData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createLeaderData(
          selfMemberIndex,
          Term(1),
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(2), candidateMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), votedFor = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, leaderData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, lastLogIndex > log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val leaderData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createLeaderData(
          selfMemberIndex,
          Term(2),
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(3), candidateMemberIndex, lastLogIndex = LogEntryIndex(3), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, leaderData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, lastLogIndex < log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val leaderData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createLeaderData(
          selfMemberIndex,
          Term(1),
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "dummy"), Term(1)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(2), candidateMemberIndex, lastLogIndex = LogEntryIndex(0), lastLogTerm = Term(0))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), votedFor = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, leaderData, requestVote, expectedReplyMessage, verifyState)
    }

  }

}

object RaftActorLeaderReceivingRequestVoteSpec {

  private def createLeaderData(
      selfMemberIndex: MemberIndex,
      currentTerm: Term,
      log: ReplicatedLog,
  ): RaftMemberData = {
    val nextIndex = NextIndex(log)
    RaftMemberData(
      currentTerm = currentTerm,
      // The leader should always vote for itself.
      votedFor = Some(selfMemberIndex),
      replicatedLog = log,
      leaderMember = None,
      nextIndex = Some(nextIndex),
    ).initializeLeaderData()
  }

  private def newReplicatedLog(
      entries: LogEntry*,
  ) = {
    ReplicatedLog()
      .reset(Term(0), LogEntryIndex(0))
      .merge(entries, LogEntryIndex(0))
  }

}
