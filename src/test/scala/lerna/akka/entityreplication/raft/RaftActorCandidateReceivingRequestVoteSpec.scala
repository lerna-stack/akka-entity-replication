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
import org.scalatest.Inside

final class RaftActorCandidateReceivingRequestVoteSpec
    extends TestKit(ActorSystem())
    with RaftActorSpecBase
    with Inside {

  import RaftActor._
  import lerna.akka.entityreplication.raft.RaftActorCandidateReceivingRequestVoteSpec._

  private val shardId = createUniqueShardId()

  private def verifyReceivingRequestVote(
      selfMemberIndex: MemberIndex,
      candidateData: RaftMemberData,
      requestVote: RequestVote,
      expectedReplyMessage: RequestVoteResponse,
      verifyState: RaftTestProbe.RaftState => Unit,
  ): Unit = {
    val candidate = createRaftActor(
      shardId = shardId,
      selfMemberIndex = selfMemberIndex,
    )
    setState(candidate, Candidate, candidateData)
    candidate ! requestVote
    expectMsg(expectedReplyMessage)
    verifyState(getState(candidate))
  }

  private def expectCandidateState(
      selfMemberIndex: MemberIndex,
      currentTerm: Term,
      votedFor: Option[MemberIndex],
  )(actual: RaftTestProbe.RaftState): Unit = {
    assert(actual.stateName === Candidate)
    assert(actual.stateData.currentTerm === currentTerm)
    assert(actual.stateData.votedFor === votedFor)
    votedFor.foreach { candidateMemberIndex =>
      assert(
        candidateMemberIndex === selfMemberIndex,
        "A candidate should become a follower after it votes for another candidate.",
      )
    }
  }

  private def expectFollowerState(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
  )(actual: RaftTestProbe.RaftState): Unit = {
    assert(actual.stateName === Follower)
    assert(actual.stateData.currentTerm === currentTerm)
    assert(actual.stateData.votedFor === votedFor)
    assert(
      actual.stateData.leaderMember === None,
      "A follower should not know the leader immediately after it comes from a candidate.",
    )
  }

  "Candidate" should {

    "accept RequestVote(term = currentTerm, candidate = self, ...)" in {
      val selfMemberIndex = createUniqueMemberIndex()

      val candidateData        = createCandidateData(Term(1), votedFor = None, acceptedMembers = Set.empty, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), selfMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectCandidateState(selfMemberIndex, Term(1), Some(selfMemberIndex))(_)

      verifyReceivingRequestVote(selfMemberIndex, candidateData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = other, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val candidateData        = createCandidateData(Term(1), votedFor = None, acceptedMembers = Set.empty, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), otherCandidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteDenied(Term(1))
      val verifyState          = expectCandidateState(selfMemberIndex, Term(1), None)(_)

      verifyReceivingRequestVote(selfMemberIndex, candidateData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, lastLogIndex = log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val candidateData = createCandidateData(
        Term(1),
        votedFor = Some(selfMemberIndex),
        acceptedMembers = Set.empty,
        newReplicatedLog(),
      )
      val requestVote          = RequestVote(shardId, Term(2), otherCandidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), Some(otherCandidateMemberIndex))(_)

      verifyReceivingRequestVote(selfMemberIndex, candidateData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val candidateData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createCandidateData(
          Term(1),
          votedFor = Some(selfMemberIndex),
          acceptedMembers = Set.empty,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          ),
        )
      }
      val requestVote          = RequestVote(shardId, Term(2), otherCandidateMemberIndex, LogEntryIndex(1), Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), None)(_)

      verifyReceivingRequestVote(selfMemberIndex, candidateData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, lastLogIndex > log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val candidateData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createCandidateData(
          Term(1),
          votedFor = Some(selfMemberIndex),
          acceptedMembers = Set.empty,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(2)),
          ),
        )
      }
      val requestVote          = RequestVote(shardId, Term(2), otherCandidateMemberIndex, LogEntryIndex(3), Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), None)(_)

      verifyReceivingRequestVote(selfMemberIndex, candidateData, requestVote, expectedReplyMessage, verifyState)
    }

    "!!! deny RequestVote(term > currentTerm, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      // TODO: This case is duplicated.
      val selfMemberIndex           = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val candidateData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createCandidateData(
          Term(1),
          votedFor = Some(selfMemberIndex),
          acceptedMembers = Set.empty,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
          ),
        )
      }
      val requestVote          = RequestVote(shardId, Term(2), otherCandidateMemberIndex, LogEntryIndex(0), Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), None)(_)

      verifyReceivingRequestVote(selfMemberIndex, candidateData, requestVote, expectedReplyMessage, verifyState)
    }

  }

}

object RaftActorCandidateReceivingRequestVoteSpec {

  private def createCandidateData(
      currentTerm: Term,
      votedFor: Option[MemberIndex],
      acceptedMembers: Set[MemberIndex],
      log: ReplicatedLog,
  ): RaftMemberData = {
    // `acceptedMembers` could be empty even if `votedFor` = Some(self)
    // since `acceptedMembers` will be updated after a candidate receives a `RequestVoteAccepted`.
    RaftMemberData(
      currentTerm = currentTerm,
      votedFor = votedFor,
      replicatedLog = log,
      leaderMember = None,
      acceptedMembers = acceptedMembers,
    ).initializeCandidateData()
  }

  private def newReplicatedLog(
      entries: LogEntry*,
  ) = {
    ReplicatedLog()
      .reset(Term(0), LogEntryIndex(0))
      .merge(entries, LogEntryIndex(0))
  }

}
