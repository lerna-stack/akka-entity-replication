package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, NoOp, ReplicatedLog, Term }
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

    "deny RequestVote(term < currentTerm, ...)" in {
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

    "accept RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(2),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(),
        )
      val requestVote          = RequestVote(shardId, Term(2), candidateMemberIndex, LogEntryIndex(1), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(3),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(3), candidateMemberIndex, LogEntryIndex(1), Term(2))
      val expectedReplyMessage = RequestVoteAccepted(Term(3), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(4),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(4), candidateMemberIndex, LogEntryIndex(1), Term(3))
      val expectedReplyMessage = RequestVoteAccepted(Term(4), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(4), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(2),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(2), candidateMemberIndex, LogEntryIndex(2), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(2),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(2), candidateMemberIndex, LogEntryIndex(1), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
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

    "deny RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(3),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(3), candidateMemberIndex, LogEntryIndex(3), Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(3),
        votedFor = None,
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        ),
      )
      val requestVote =
        RequestVote(shardId, Term(3), candidateMemberIndex, lastLogIndex = LogEntryIndex(2), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = newCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(3),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(3), candidateMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(2),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(),
        )
      val requestVote          = RequestVote(shardId, Term(2), votedMemberIndex, LogEntryIndex(1), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(3),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(3), votedMemberIndex, LogEntryIndex(1), Term(2))
      val expectedReplyMessage = RequestVoteAccepted(Term(3), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(4),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(4), votedMemberIndex, LogEntryIndex(1), Term(3))
      val expectedReplyMessage = RequestVoteAccepted(Term(4), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(4), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(2),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(2), votedMemberIndex, LogEntryIndex(2), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(2), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), votedMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(2),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(2), votedMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(2))
      val verifyState          = expectFollowerState(Term(2), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(
          Term(3),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      val requestVote          = RequestVote(shardId, Term(3), votedMemberIndex, LogEntryIndex(3), Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(3),
        votedFor = Some(votedMemberIndex),
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        ),
      )
      val requestVote =
        RequestVote(shardId, Term(3), votedMemberIndex, lastLogIndex = LogEntryIndex(2), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = sameCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex  = createUniqueMemberIndex()
      val votedMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(3),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(3), votedMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(votedMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term = currentTerm, candidate = otherCandidate, ...)" in {
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

    "accept RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData         = createFollowerData(Term(0), votedFor = None, leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), candidateMemberIndex, LogEntryIndex(1), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(2),
        votedFor = None,
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        ),
      )
      val requestVote          = RequestVote(shardId, Term(3), candidateMemberIndex, LogEntryIndex(1), Term(2))
      val expectedReplyMessage = RequestVoteAccepted(Term(3), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(3),
        votedFor = None,
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        ),
      )
      val requestVote          = RequestVote(shardId, Term(4), candidateMemberIndex, LogEntryIndex(1), Term(3))
      val expectedReplyMessage = RequestVoteAccepted(Term(4), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(4), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(2),
        votedFor = None,
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        ),
      )
      val requestVote          = RequestVote(shardId, Term(3), candidateMemberIndex, LogEntryIndex(2), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(3), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData         = createFollowerData(Term(0), votedFor = None, leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), candidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(candidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
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

    "deny RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        val entityId = NormalizedEntityId.from("test-entity")
        createFollowerData(
          Term(3),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(4), candidateMemberIndex, lastLogIndex = LogEntryIndex(3), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(4))
      val verifyState          = expectFollowerState(Term(4), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex      = createUniqueMemberIndex()
      val candidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(3),
          votedFor = None,
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(4), candidateMemberIndex, lastLogIndex = LogEntryIndex(2), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(4))
      val verifyState          = expectFollowerState(Term(4), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, candidate = newCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
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

    "accept RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData =
        createFollowerData(Term(0), votedFor = Some(votedMemberIndex), leaderMember = None, newReplicatedLog())
      val requestVote          = RequestVote(shardId, Term(1), otherCandidateMemberIndex, LogEntryIndex(1), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(1), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(1), votedFor = Some(otherCandidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(2),
        votedFor = Some(votedMemberIndex),
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        ),
      )
      val requestVote          = RequestVote(shardId, Term(3), otherCandidateMemberIndex, LogEntryIndex(1), Term(2))
      val expectedReplyMessage = RequestVoteAccepted(Term(3), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(otherCandidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm > log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(3),
        votedFor = Some(votedMemberIndex),
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        ),
      )
      val requestVote          = RequestVote(shardId, Term(4), otherCandidateMemberIndex, LogEntryIndex(1), Term(3))
      val expectedReplyMessage = RequestVoteAccepted(Term(4), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(4), votedFor = Some(otherCandidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = createFollowerData(
        Term(2),
        votedFor = Some(votedMemberIndex),
        leaderMember = None,
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        ),
      )
      val requestVote          = RequestVote(shardId, Term(3), otherCandidateMemberIndex, LogEntryIndex(2), Term(1))
      val expectedReplyMessage = RequestVoteAccepted(Term(3), selfMemberIndex)
      val verifyState          = expectFollowerState(Term(3), votedFor = Some(otherCandidateMemberIndex), leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "accept RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
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

    "deny RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm = log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(3),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(4), otherCandidateMemberIndex, lastLogIndex = LogEntryIndex(1), lastLogTerm = Term(2))
      val expectedReplyMessage = RequestVoteDenied(Term(4))
      val verifyState          = expectFollowerState(Term(4), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex > log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(3),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(4), otherCandidateMemberIndex, lastLogIndex = LogEntryIndex(3), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(4))
      val verifyState          = expectFollowerState(Term(4), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex = log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(3),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
      }
      val requestVote =
        RequestVote(shardId, Term(4), otherCandidateMemberIndex, lastLogIndex = LogEntryIndex(2), lastLogTerm = Term(1))
      val expectedReplyMessage = RequestVoteDenied(Term(4))
      val verifyState          = expectFollowerState(Term(4), votedFor = None, leaderMember = None)(_)

      verifyReceivingRequestVote(selfMemberIndex, followerData, requestVote, expectedReplyMessage, verifyState)
    }

    "deny RequestVote(term > currentTerm, candidate = otherCandidate, lastLogIndex < log.lastLogIndex, lastLogTerm < log.lastLogTerm, ...)" in {
      val selfMemberIndex           = createUniqueMemberIndex()
      val votedMemberIndex          = createUniqueMemberIndex()
      val otherCandidateMemberIndex = createUniqueMemberIndex()

      val followerData = {
        createFollowerData(
          Term(2),
          votedFor = Some(votedMemberIndex),
          leaderMember = None,
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
      }
      val requestVote          = RequestVote(shardId, Term(3), otherCandidateMemberIndex, LogEntryIndex(0), Term(0))
      val expectedReplyMessage = RequestVoteDenied(Term(3))
      val verifyState          = expectFollowerState(Term(3), votedFor = None, leaderMember = None)(_)

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
