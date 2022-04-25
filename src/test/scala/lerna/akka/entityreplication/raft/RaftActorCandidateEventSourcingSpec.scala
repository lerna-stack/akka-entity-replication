package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model._
import org.scalatest.Inside

final class RaftActorCandidateEventSourcingSpec extends TestKit(ActorSystem()) with RaftActorSpecBase with Inside {

  import RaftActor._
  import RaftActorCandidateEventSourcingSpec._
  import eventsourced.CommitLogStoreActor._

  private def spawnCandidate(
      currentTerm: Term,
      replicatedLog: ReplicatedLog,
      commitIndex: LogEntryIndex,
      lastApplied: LogEntryIndex,
      eventSourcingIndex: Option[LogEntryIndex],
  ): RaftTestFSMRef = {
    val candidate = createRaftActor(
      shardId = createUniqueShardId(),
      selfMemberIndex = createUniqueMemberIndex(),
    )
    val candidateData = createCandidateData(
      currentTerm = currentTerm,
      replicatedLog = replicatedLog,
      commitIndex = commitIndex,
      lastApplied = lastApplied,
      eventSourcingIndex = eventSourcingIndex,
    )
    setState(candidate, Candidate, candidateData)
    candidate
  }

  "Candidate" should {

    "handle AppendCommittedEntriesResponse(index=0) and update its eventSourcingIndex when it has no eventSourcingIndex" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val candidate = spawnCandidate(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = None,
      )
      candidate ! AppendCommittedEntriesResponse(LogEntryIndex(0))
      getState(candidate).stateData.eventSourcingIndex should be(Some(LogEntryIndex(0)))
    }

    "handle AppendCommittedEntriesResponse(index=1) and update its eventSourcingIndex when it has no eventSourcingIndex" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val candidate = spawnCandidate(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = None,
      )
      candidate ! AppendCommittedEntriesResponse(LogEntryIndex(1))
      getState(candidate).stateData.eventSourcingIndex should be(Some(LogEntryIndex(1)))
    }

    "handle AppendCommittedEntriesResponse(index=2) and update its eventSourcingIndex when its eventSourcingIndex is 1" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val candidate = spawnCandidate(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = Some(LogEntryIndex(1)),
      )
      candidate ! AppendCommittedEntriesResponse(LogEntryIndex(2))
      getState(candidate).stateData.eventSourcingIndex should be(Some(LogEntryIndex(2)))
    }

    "handle AppendCommittedEntriesResponse(index=2) and update nothing when its eventSourcingIndex is 2" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val candidate = spawnCandidate(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = Some(LogEntryIndex(2)),
      )
      candidate ! AppendCommittedEntriesResponse(LogEntryIndex(2))
      getState(candidate).stateData.eventSourcingIndex should be(Some(LogEntryIndex(2)))
    }

    "handle AppendCommittedEntriesResponse(index=1) and update nothing when its eventSourcingIndex is 2" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val candidate = spawnCandidate(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = Some(LogEntryIndex(2)),
      )
      candidate ! AppendCommittedEntriesResponse(LogEntryIndex(1))
      getState(candidate).stateData.eventSourcingIndex should be(Some(LogEntryIndex(2)))
    }

  }

}

object RaftActorCandidateEventSourcingSpec {

  private def createCandidateData(
      currentTerm: Term,
      replicatedLog: ReplicatedLog,
      commitIndex: LogEntryIndex,
      lastApplied: LogEntryIndex,
      eventSourcingIndex: Option[LogEntryIndex],
  ): RaftMemberData = {
    require(
      currentTerm > Term(0),
      s"Candidate should have a term higher than Term(0).",
    )
    require(
      lastApplied <= commitIndex,
      s"lastApplied [$lastApplied] should be less than or equal to commitIndex [$commitIndex]",
    )
    require(
      currentTerm >= replicatedLog.lastLogTerm,
      s"currentTerm [$currentTerm] should be greater than or equal to ReplicatedLog.lastLogTerm [${replicatedLog.lastLogTerm}]",
    )
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = replicatedLog,
      commitIndex = commitIndex,
      lastApplied = lastApplied,
      eventSourcingIndex = eventSourcingIndex,
    ).initializeCandidateData()
  }

  private def newReplicatedLog(
      entries: LogEntry*,
  ): ReplicatedLog = {
    ReplicatedLog()
      .reset(Term(0), LogEntryIndex(0))
      .truncateAndAppend(entries)
  }

}
