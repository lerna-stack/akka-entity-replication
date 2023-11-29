package lerna.akka.entityreplication.raft

import akka.actor.ActorRef
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.RaftMemberData.SnapshotSynchronizationDecision
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands.InstallSnapshot
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Inside, Matchers }

import java.util.UUID

final class RaftMemberDataSpec extends FlatSpec with Matchers with Inside with MockFactory {

  behavior of "RaftMemberData"

  it should "return entries on selectEntityEntries when the entries following with the condition exists" in {
    val entityId1 = generateEntityId()
    val entityId2 = generateEntityId()
    val term      = Term.initial().next()
    val logEntries = Seq(
      LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), term),
      LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "a"), term),
      LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId2), "b"), term),
      LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId1), "c"), term),
      LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId2), "d"), term),
      LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId1), "e"), term),
      LogEntry(LogEntryIndex(7), EntityEvent(Option(entityId2), "f"), term),
    )
    val data = RaftMemberData(
      replicatedLog = ReplicatedLog().truncateAndAppend(logEntries),
      lastApplied = LogEntryIndex(5),
    )
    val selectedForEntity1 =
      data.selectEntityEntries(entityId = entityId1, from = LogEntryIndex(2), to = data.lastApplied)

    selectedForEntity1.map(_.index) should be(Seq(LogEntryIndex(2), LogEntryIndex(4)))

    val selectedForEntity2 =
      data.selectEntityEntries(entityId = entityId2, from = LogEntryIndex(4), to = data.lastApplied)

    selectedForEntity2.map(_.index) should be(Seq(LogEntryIndex(5)))
  }

  it should "not return any entities on selectEntityEntries when the entries following with the condition doesn't exist" in {
    val entityId = generateEntityId()
    val term     = Term.initial().next()
    val logEntries = Seq(
      LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), term),
    )
    val data = RaftMemberData(
      replicatedLog = ReplicatedLog().truncateAndAppend(logEntries),
      lastApplied = LogEntryIndex(1),
    )
    val selected =
      data.selectEntityEntries(entityId = entityId, from = LogEntryIndex.initial(), to = data.lastApplied)

    selected.map(_.index) should be(empty)
  }

  it should "produce IllegalArgumentException on selectEntityEntries when 'to' index is greater than lastApplied" in {
    val entityId1 = generateEntityId()
    val entityId2 = generateEntityId()
    val term      = Term.initial().next()
    val logEntries = Seq(
      LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), term),
      LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "a"), term),
      LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId2), "b"), term),
      LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId1), "c"), term),
      LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId2), "d"), term),
      LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId1), "e"), term),
      LogEntry(LogEntryIndex(7), EntityEvent(Option(entityId2), "f"), term),
    )
    val data = RaftMemberData(
      replicatedLog = ReplicatedLog().truncateAndAppend(logEntries),
      lastApplied = LogEntryIndex(5),
    )

    val toIndex = LogEntryIndex(6)
    assume(data.lastApplied < toIndex)

    val ex =
      intercept[IllegalArgumentException] {
        data.selectEntityEntries(entityId = entityId1, from = LogEntryIndex.initial(), to = toIndex)
      }
    ex.getMessage should include(
      "Cannot select the entries (0-6) unless RaftActor have applied the entries to the entities (lastApplied: 5)",
    )
  }

  it should "return true on willGetMatchSnapshots when 'prevLogTerm' and 'prevLogIndex' match with 'targetSnapshotLastTerm' and 'targetSnapshotLastLogIndex'" in {
    val data = RaftMemberData(
      lastSnapshotStatus = SnapshotStatus(
        snapshotLastTerm = Term.initial(),
        snapshotLastLogIndex = LogEntryIndex.initial(),
        targetSnapshotLastTerm = Term(20),
        targetSnapshotLastLogIndex = LogEntryIndex(100),
      ),
    )
    data.willGetMatchSnapshots(prevLogTerm = Term(1), prevLogIndex = LogEntryIndex(3)) should be(false)
    data.willGetMatchSnapshots(prevLogTerm = Term(20), prevLogIndex = LogEntryIndex(3)) should be(false)
    data.willGetMatchSnapshots(prevLogTerm = Term(1), prevLogIndex = LogEntryIndex(100)) should be(false)
    data.willGetMatchSnapshots(prevLogTerm = Term(20), prevLogIndex = LogEntryIndex(100)) should be(true)
  }

  behavior of "RaftMemberData.updateEventSourcingIndex"

  it should "return new RaftMemberData with the given new eventSourcingIndex when it has no eventSourcingIndex" in {
    val data = RaftMemberData()
    data.eventSourcingIndex should be(None)

    val newDataWithIndex0 = data.updateEventSourcingIndex(LogEntryIndex(0))
    data.eventSourcingIndex should be(None)
    newDataWithIndex0.eventSourcingIndex should be(Some(LogEntryIndex(0)))

    val newDataWithIndex3 = data.updateEventSourcingIndex(LogEntryIndex(3))
    data.eventSourcingIndex should be(None)
    newDataWithIndex3.eventSourcingIndex should be(Some(LogEntryIndex(3)))
  }

  it should "return new RaftMemberData with the given new eventSourcingIndex when the given index is greater than the current one" in {
    val data = RaftMemberData(eventSourcingIndex = Some(LogEntryIndex(1)))
    data.eventSourcingIndex should be(Some(LogEntryIndex(1)))

    val newDataWithIndex2 = data.updateEventSourcingIndex(LogEntryIndex(2))
    data.eventSourcingIndex should be(Some(LogEntryIndex(1)))
    newDataWithIndex2.eventSourcingIndex should be(Some(LogEntryIndex(2)))

    val newDataWithIndex3 = data.updateEventSourcingIndex(LogEntryIndex(3))
    data.eventSourcingIndex should be(Some(LogEntryIndex(1)))
    newDataWithIndex3.eventSourcingIndex should be(Some(LogEntryIndex(3)))
  }

  it should "throw IllegalArgumentException when the given new eventSourcingIndex equals the current eventSourcingIndex" in {
    val data = RaftMemberData(eventSourcingIndex = Some(LogEntryIndex(3)))
    data.eventSourcingIndex should be(Some(LogEntryIndex(3)))

    val exceptionWithIndex3 = intercept[IllegalArgumentException] {
      data.updateEventSourcingIndex(LogEntryIndex(3))
    }
    data.eventSourcingIndex should be(Some(LogEntryIndex(3)))
    exceptionWithIndex3.getMessage should be(
      "requirement failed: eventSourcingIndex should only increase. " +
      "The given index [3] is less than or equal to the current index [3].",
    )
  }

  it should "throw IllegalArgumentException when the given new eventSourcingIndex is less than eventSourcingIndex" in {
    val data = RaftMemberData(eventSourcingIndex = Some(LogEntryIndex(3)))
    data.eventSourcingIndex should be(Some(LogEntryIndex(3)))

    val exceptionWithIndex2 = intercept[IllegalArgumentException] {
      data.updateEventSourcingIndex(LogEntryIndex(2))
    }
    data.eventSourcingIndex should be(Some(LogEntryIndex(3)))
    exceptionWithIndex2.getMessage should be(
      "requirement failed: eventSourcingIndex should only increase. " +
      "The given index [2] is less than or equal to the current index [3].",
    )
  }

  behavior of "RaftMemberData.estimatedReplicatedLogSizeAfterCompaction"

  it should "return estimated compacted log size when lastApplied is greater than eventSourcingIndex" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(3),
      lastApplied = LogEntryIndex(3),
      eventSourcingIndex = Some(LogEntryIndex(2)),
    )
    data.estimatedReplicatedLogSizeAfterCompaction(1) should be(2)
  }

  it should "return estimated compacted log size when lastApplied is less than eventSourcingIndex" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(3),
      lastApplied = LogEntryIndex(3),
      eventSourcingIndex = Some(LogEntryIndex(4)),
    )
    data.estimatedReplicatedLogSizeAfterCompaction(1) should be(2)
  }

  it should "return estimated compacted log size when eventSourcingIndex is unknown" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(2),
      lastApplied = LogEntryIndex(2),
      eventSourcingIndex = None,
    )
    data.estimatedReplicatedLogSizeAfterCompaction(1) should be(2)
  }

  it should "return estimated compacted log size (preserveLogSize floors this size)" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event2"), Term(1)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(2),
      lastApplied = LogEntryIndex(2),
      eventSourcingIndex = Some(LogEntryIndex(2)),
    )
    data.estimatedReplicatedLogSizeAfterCompaction(1) should be(1)
    data.estimatedReplicatedLogSizeAfterCompaction(2) should be(2)
    data.estimatedReplicatedLogSizeAfterCompaction(3) should be(3)
    data.estimatedReplicatedLogSizeAfterCompaction(4) should be(3)
  }

  it should "throw an IllegalArgumentException if the given preserveLogSize is less than or equals to 0" in {
    val data = {
      val replicatedLog =
        ReplicatedLog().truncateAndAppend(Seq(LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1))))
      RaftMemberData(
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(1),
        lastApplied = LogEntryIndex(1),
        eventSourcingIndex = None,
      )
    }

    val exceptionWithZero = intercept[IllegalArgumentException] {
      data.estimatedReplicatedLogSizeAfterCompaction(0)
    }
    exceptionWithZero.getMessage should be("requirement failed: preserveLogSize(0) should be greater than 0.")

    val exceptionWithMinusOne = intercept[IllegalArgumentException] {
      data.estimatedReplicatedLogSizeAfterCompaction(-1)
    }
    exceptionWithMinusOne.getMessage should be("requirement failed: preserveLogSize(-1) should be greater than 0.")
  }

  behavior of "RaftMemberData.compactReplicatedLog"

  it should "return new RaftMemberData with compacted entries. The number of compacted entries should be greater than or equal to preserveLogSize)" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(3),
      lastApplied = LogEntryIndex(3),
      lastSnapshotStatus = nonDirtySnapshotStatus(Term(2), LogEntryIndex(3)),
      eventSourcingIndex = Some(LogEntryIndex(3)),
    )

    inside(data.compactReplicatedLog(preserveLogSize = 1).replicatedLog) {
      case newReplicatedLog =>
        newReplicatedLog.ancestorLastTerm should be(Term(2))
        newReplicatedLog.ancestorLastIndex should be(LogEntryIndex(3))
        newReplicatedLog.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          newReplicatedLog.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
          newReplicatedLog.entries(1),
        )
    }

    inside(data.compactReplicatedLog(preserveLogSize = 3).replicatedLog) {
      case newReplicatedLog =>
        newReplicatedLog.ancestorLastTerm should be(Term(1))
        newReplicatedLog.ancestorLastIndex should be(LogEntryIndex(2))
        newReplicatedLog.entries.size should be(3)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          newReplicatedLog.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          newReplicatedLog.entries(1),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
          newReplicatedLog.entries(2),
        )
    }

    inside(data.compactReplicatedLog(preserveLogSize = 6).replicatedLog) {
      case newReplicatedLog =>
        newReplicatedLog.ancestorLastTerm should be(Term(0))
        newReplicatedLog.ancestorLastIndex should be(LogEntryIndex(0))
        newReplicatedLog.entries.size should be(5)
        (0 until 5).foreach { i =>
          assertEqualsLogEntry(
            replicatedLog.entries(i),
            newReplicatedLog.entries(i),
          )
        }
    }
  }

  it should "return new RaftMemberData with compacted entries. The new data should also contain entries with indices between eventSourcingIndex+1 and lastApplied." in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(3),
      lastApplied = LogEntryIndex(3),
      lastSnapshotStatus = nonDirtySnapshotStatus(Term(2), LogEntryIndex(3)),
      eventSourcingIndex = Some(LogEntryIndex(2)),
    )
    inside(data.compactReplicatedLog(preserveLogSize = 2).replicatedLog) {
      case newReplicatedLog =>
        newReplicatedLog.ancestorLastTerm should be(Term(1))
        newReplicatedLog.ancestorLastIndex should be(LogEntryIndex(2))
        newReplicatedLog.entries.size should be(3)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          newReplicatedLog.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          newReplicatedLog.entries(1),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
          newReplicatedLog.entries(2),
        )
    }
  }

  it should "return new RaftMemberData with whole entries when eventSourcingIndex is unknown" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog = {
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event2"), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event3"), Term(2)),
        ),
      )
    }
    val data = RaftMemberData(
      replicatedLog = replicatedLog,
      commitIndex = LogEntryIndex(3),
      lastApplied = LogEntryIndex(3),
      lastSnapshotStatus = nonDirtySnapshotStatus(Term(2), LogEntryIndex(3)),
      eventSourcingIndex = None,
    )
    inside(data.compactReplicatedLog(preserveLogSize = 2).replicatedLog) {
      case newReplicatedLog =>
        newReplicatedLog.ancestorLastTerm should be(Term(0))
        newReplicatedLog.ancestorLastIndex should be(LogEntryIndex(0))
        newReplicatedLog.entries.size should be(5)
        (0 until 5).foreach { i =>
          assertEqualsLogEntry(replicatedLog.entries(i), newReplicatedLog.entries(i))
        }
    }
  }

  it should "throw an IllegalArgumentException if the given preserveLogSize is less than or equals to 0" in {
    val data = {
      val replicatedLog =
        ReplicatedLog().truncateAndAppend(Seq(LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1))))
      RaftMemberData(
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(1),
        lastApplied = LogEntryIndex(1),
        eventSourcingIndex = None,
      )
    }

    val exceptionWithZero = intercept[IllegalArgumentException] {
      data.compactReplicatedLog(0)
    }
    exceptionWithZero.getMessage should be("requirement failed: preserveLogSize(0) should be greater than 0.")

    val exceptionWithMinusOne = intercept[IllegalArgumentException] {
      data.compactReplicatedLog(-1)
    }
    exceptionWithMinusOne.getMessage should be("requirement failed: preserveLogSize(-1) should be greater than 0.")
  }

  behavior of "RaftMemberData.decideSnapshotSync"

  it should "throw an IllegalArgumentException if InstallSnapshot.term is less than currentTerm" in {
    val data = RaftMemberData(currentTerm = Term(10))
    val exception = intercept[IllegalArgumentException] {
      data.decideSnapshotSync(
        InstallSnapshot(
          NormalizedShardId.from("shard-1"),
          Term(9),
          MemberIndex("member-1"),
          srcLatestSnapshotLastLogTerm = Term(4),
          srcLatestSnapshotLastLogLogIndex = LogEntryIndex(123),
        ),
      )
    }
    exception.getMessage should be("requirement failed: InstallSnapshot.term [9] should be equal to currentTerm [10]")
  }

  it should "throw an IllegalArgumentException if InstallSnapshot.term is greater than currentTerm" in {
    val data = RaftMemberData(currentTerm = Term(10))
    val exception = intercept[IllegalArgumentException] {
      data.decideSnapshotSync(
        InstallSnapshot(
          NormalizedShardId.from("shard-1"),
          Term(11),
          MemberIndex("member-1"),
          srcLatestSnapshotLastLogTerm = Term(4),
          srcLatestSnapshotLastLogLogIndex = LogEntryIndex(123),
        ),
      )
    }
    exception.getMessage should be("requirement failed: InstallSnapshot.term [11] should be equal to currentTerm [10]")
  }

  it should "return StartDecision " +
  "if the given InstallSnapshot message conflicts with no existing log entries" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val decision = data.decideSnapshotSync(
      InstallSnapshot(
        NormalizedShardId.from("shard-1"),
        Term(10),
        MemberIndex("member-1"),
        srcLatestSnapshotLastLogTerm = Term(8),
        srcLatestSnapshotLastLogLogIndex = LogEntryIndex(18),
      ),
    )
    decision should be(SnapshotSynchronizationDecision.StartDecision)
  }

  it should "return StartDecision" +
  "if the given InstallSnapshot message conflicts with an existing uncommitted log entry" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val decision = data.decideSnapshotSync(
      InstallSnapshot(
        NormalizedShardId.from("shard-1"),
        Term(10),
        MemberIndex("member-1"),
        srcLatestSnapshotLastLogTerm = Term(9),
        srcLatestSnapshotLastLogLogIndex = LogEntryIndex(17),
      ),
    )
    decision should be(SnapshotSynchronizationDecision.StartDecision)
  }

  it should "return SkipDecision(matchIndex=InstallSnapshot.srcLatestSnapshotLastLogLogIndex) if: " +
  "the given InstallSnapshot message installs no new snapshots, and " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex >= replicatedLog.ancestorLastIndex, and " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex > commitIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val decision = data.decideSnapshotSync(
      InstallSnapshot(
        NormalizedShardId.from("shard-1"),
        Term(10),
        MemberIndex("member-1"),
        srcLatestSnapshotLastLogTerm = Term(8),
        srcLatestSnapshotLastLogLogIndex = LogEntryIndex(17),
      ),
    )
    decision should be(SnapshotSynchronizationDecision.SkipDecision(LogEntryIndex(17)))
  }

  it should "return SkipDecision(matchIndex=commitIndex) if: " +
  "the given InstallSnapshot message installs no new snapshots, and " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex >= replicatedLog.ancestorLastIndex, and " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex < commitIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val decision = data.decideSnapshotSync(
      InstallSnapshot(
        NormalizedShardId.from("shard-1"),
        Term(10),
        MemberIndex("member-1"),
        srcLatestSnapshotLastLogTerm = Term(7),
        srcLatestSnapshotLastLogLogIndex = LogEntryIndex(15),
      ),
    )
    decision should be(SnapshotSynchronizationDecision.SkipDecision(LogEntryIndex(16)))
  }

  it should "return SkipDecision(matchIndex=replicatedLog.ancestorLastIndex) if: " +
  "the given InstallSnapshot message installs no new snapshots, and " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex < replicatedLog.ancestorLastIndex, and " +
  "replicatedLog.ancestorLastIndex > commitIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(0),
      )
    }
    val decision = data.decideSnapshotSync(
      InstallSnapshot(
        NormalizedShardId.from("shard-1"),
        Term(10),
        MemberIndex("member-1"),
        srcLatestSnapshotLastLogTerm = Term(7),
        srcLatestSnapshotLastLogLogIndex = LogEntryIndex(13),
      ),
    )
    decision should be(SnapshotSynchronizationDecision.SkipDecision(LogEntryIndex(14)))
  }

  it should "return SkipDecision(matchIndex=commitIndex) if: " +
  "the given InstallSnapshot message installs no new snapshots, and " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex < replicatedLog.ancestorLastIndex, and " +
  "replicatedLog.ancestorLastIndex < commitIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val decision = data.decideSnapshotSync(
      InstallSnapshot(
        NormalizedShardId.from("shard-1"),
        Term(10),
        MemberIndex("member-1"),
        srcLatestSnapshotLastLogTerm = Term(7),
        srcLatestSnapshotLastLogLogIndex = LogEntryIndex(13),
      ),
    )
    decision should be(SnapshotSynchronizationDecision.SkipDecision(LogEntryIndex(16)))
  }

  it should "return ErrorDecision if the InstallSnapshot message conflicts with a compacted log entry: " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex == replicatedLog.ancestorLastIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val installSnapshot = InstallSnapshot(
      NormalizedShardId.from("shard-1"),
      Term(10),
      MemberIndex("member-1"),
      srcLatestSnapshotLastLogTerm = Term(6),
      srcLatestSnapshotLastLogLogIndex = LogEntryIndex(14),
    )
    val decision = data.decideSnapshotSync(installSnapshot)
    inside(decision) {
      case SnapshotSynchronizationDecision.ErrorDecision(reason) =>
        reason should be(
          s"[${installSnapshot}] conflicted with a compacted (committed) entry of ReplicatedLog(ancestorLastTerm=[7], ancestorLastIndex=[14])",
        )
    }
  }

  it should "return ErrorDecision if the InstallSnapshot message conflicts with a compacted log entry: " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex < replicatedLog.ancestorLastIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val installSnapshot = InstallSnapshot(
      NormalizedShardId.from("shard-1"),
      Term(10),
      MemberIndex("member-1"),
      srcLatestSnapshotLastLogTerm = Term(8),
      srcLatestSnapshotLastLogLogIndex = LogEntryIndex(13),
    )
    val decision = data.decideSnapshotSync(installSnapshot)
    inside(decision) {
      case SnapshotSynchronizationDecision.ErrorDecision(reason) =>
        reason should be(
          s"[${installSnapshot}] conflicted with a compacted (committed) entry of " +
          "ReplicatedLog(ancestorLastTerm=[7], ancestorLastIndex=[14]) since terms only increase",
        )
    }
  }

  it should "return ErrorDecision if the InstallSnapshot message conflicts with a committed log entry: " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex < commitIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val installSnapshot = InstallSnapshot(
      NormalizedShardId.from("shard-1"),
      Term(10),
      MemberIndex("member-1"),
      srcLatestSnapshotLastLogTerm = Term(6),
      srcLatestSnapshotLastLogLogIndex = LogEntryIndex(15),
    )
    val decision = data.decideSnapshotSync(installSnapshot)
    inside(decision) {
      case SnapshotSynchronizationDecision.ErrorDecision(reason) =>
        reason should be(
          s"[${installSnapshot}] conflicted with a committed entry (term=[7]). commitIndex=[16]",
        )
    }
  }

  it should "return ErrorDecision if the InstallSnapshot message conflicts with a committed log entry: " +
  "InstallSnapshot.srcLatestSnapshotLastLogLogIndex == commitIndex" in {
    val data = {
      val replicatedLog =
        ReplicatedLog()
          .reset(Term(7), LogEntryIndex(14))
          .truncateAndAppend(
            Seq(
              LogEntry(LogEntryIndex(15), EntityEvent(Some(NormalizedEntityId.from("entity-1")), "event-a"), Term(7)),
              LogEntry(LogEntryIndex(16), EntityEvent(None, NoOp), Term(8)),
              LogEntry(LogEntryIndex(17), EntityEvent(Some(NormalizedEntityId.from("entity-2")), "event-b"), Term(8)),
            ),
          )
      RaftMemberData(
        currentTerm = Term(10),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(16),
      )
    }
    val installSnapshot = InstallSnapshot(
      NormalizedShardId.from("shard-1"),
      Term(10),
      MemberIndex("member-1"),
      srcLatestSnapshotLastLogTerm = Term(7),
      srcLatestSnapshotLastLogLogIndex = LogEntryIndex(16),
    )
    val decision = data.decideSnapshotSync(installSnapshot)
    inside(decision) {
      case SnapshotSynchronizationDecision.ErrorDecision(reason) =>
        reason should be(
          s"[${installSnapshot}] conflicted with a committed entry (term=[8]). commitIndex=[16]",
        )
    }
  }

  behavior of "RaftMemberData.resolveCommittedEntriesForEventSourcing"

  it should "return UnknownCurrentEventSourcingIndex when it has no eventSourcingIndex" in {
    import RaftMemberData.CommittedEntriesForEventSourcingResolveError._
    val data = RaftMemberData(eventSourcingIndex = None)
    data.resolveCommittedEntriesForEventSourcing should be(Left(UnknownCurrentEventSourcingIndex))
  }

  it should "return empty entries when eventSourcingIndex equals commitIndex" in {
    val replicatedLog = {
      val entityId = NormalizedEntityId("entity1")
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event2"), Term(1)),
        ),
      )
    }
    val data = RaftMemberData(
      eventSourcingIndex = Some(LogEntryIndex(2)),
      commitIndex = LogEntryIndex(2),
      replicatedLog = replicatedLog,
    )
    data.resolveCommittedEntriesForEventSourcing should be(Right(IndexedSeq.empty))
  }

  it should "return empty entries when eventSourcingIndex is larger than commitIndex" in {
    val replicatedLog = {
      val entityId = NormalizedEntityId("entity1")
      ReplicatedLog().truncateAndAppend(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event2"), Term(1)),
        ),
      )
    }
    val data = RaftMemberData(
      eventSourcingIndex = Some(LogEntryIndex(3)),
      commitIndex = LogEntryIndex(2),
      replicatedLog = replicatedLog,
    )
    data.resolveCommittedEntriesForEventSourcing should be(Right(IndexedSeq.empty))
  }

  it should "return NextCommittedEntryNotFound when its entries are empty and eventSourcingIndex is less than commitIndex" in {
    import RaftMemberData.CommittedEntriesForEventSourcingResolveError._
    val replicatedLog = {
      val entityId = NormalizedEntityId("entity1")
      ReplicatedLog()
        .reset(Term(1), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event4"), Term(2)),
            LogEntry(LogEntryIndex(6), EntityEvent(Some(entityId), "event5"), Term(2)),
          ),
        )
    }
    val data = RaftMemberData(
      eventSourcingIndex = Some(LogEntryIndex(1)),
      commitIndex = LogEntryIndex(3),
      replicatedLog = replicatedLog,
    )
    data.resolveCommittedEntriesForEventSourcing should be(
      Left(NextCommittedEntryNotFound(LogEntryIndex(2), None)),
    )
  }

  it should "return NextCommittedEntryNotFound when its entries don't contains the next entry" in {
    import RaftMemberData.CommittedEntriesForEventSourcingResolveError._
    val replicatedLog = {
      val entityId = NormalizedEntityId("entity1")
      ReplicatedLog()
        .reset(Term(1), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event4"), Term(2)),
            LogEntry(LogEntryIndex(6), EntityEvent(Some(entityId), "event5"), Term(2)),
          ),
        )
    }
    val data = RaftMemberData(
      eventSourcingIndex = Some(LogEntryIndex(1)),
      commitIndex = LogEntryIndex(5),
      replicatedLog = replicatedLog,
    )
    data.resolveCommittedEntriesForEventSourcing should be(
      Left(NextCommittedEntryNotFound(LogEntryIndex(2), Some(LogEntryIndex(4)))),
    )
  }

  it should "return entries when its entries contains the next entry" in {
    val entityId = NormalizedEntityId("entity1")
    val replicatedLog =
      ReplicatedLog()
        .reset(Term(1), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event4"), Term(2)),
            LogEntry(LogEntryIndex(6), EntityEvent(Some(entityId), "event5"), Term(2)),
          ),
        )
    val data = RaftMemberData(
      eventSourcingIndex = Some(LogEntryIndex(3)),
      commitIndex = LogEntryIndex(5),
      replicatedLog = replicatedLog,
    )
    inside(data.resolveCommittedEntriesForEventSourcing) {
      case Right(entries) =>
        entries.size should be(2)
        assertEqualsLogEntry(LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(2)), entries(0))
        assertEqualsLogEntry(LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event4"), Term(2)), entries(1))
    }
  }

  behavior of "RaftMemberData.resolveNewLogEntries"

  it should "throws an IllegalArgumentException if the given entries start with an index greater than lastLogIndex + 1" in {
    val data = RaftMemberData(replicatedLog =
      ReplicatedLog()
        .reset(Term(2), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          ),
        ),
    )

    val exception = intercept[IllegalArgumentException] {
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(5)),
          LogEntry(LogEntryIndex(8), EntityEvent(None, NoOp), Term(6)),
        ),
      )
    }
    exception.getMessage should be(
      "requirement failed: The given non-empty log entries (indices: [7..8]) should start with an index less than or equal to lastLogIndex[5] + 1. " +
      "If this requirement breaks, the raft log will miss some entries.",
    )
  }

  it should "return empty entries if the given entries are empty" in {
    val data = RaftMemberData(replicatedLog =
      ReplicatedLog()
        .reset(Term(2), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          ),
        ),
    )

    val newEntries = data.resolveNewLogEntries(Seq.empty)
    newEntries should be(empty)
  }

  it should "return the whole new entries starting with lastLogIndex + 1 if the given entries contain no existing ones" in {
    val data = RaftMemberData(replicatedLog =
      ReplicatedLog()
        .reset(Term(2), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          ),
        ),
    )

    val entries = Seq(
      LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(4)),
      LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(4)),
      LogEntry(LogEntryIndex(8), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(4)),
    )
    val newEntries = data.resolveNewLogEntries(entries)
    newEntries should contain theSameElementsInOrderAs entries
  }

  it should "return only new entries starting with lastLogIndex + 1 if the given entries contain some existing ones" in {
    val data = RaftMemberData(replicatedLog =
      ReplicatedLog()
        .reset(Term(2), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
            LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(3)),
          ),
        ),
    )

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(8), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(4)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should contain theSameElementsInOrderAs Seq(
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(8), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(4)),
        )
    }

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should contain theSameElementsInOrderAs Seq(
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
        )
    }

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(3)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should be(empty)
    }

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should be(empty)
    }
  }

  it should "return only new entries (beginning with the first conflict) if the given entries conflict with existing entries" in {
    val data = RaftMemberData(
      replicatedLog = ReplicatedLog()
        .reset(Term(2), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
            LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("2")), "event-2"), Term(3)),
          ),
        ),
      commitIndex = LogEntryIndex(4),
    )

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("2")), "event-2"), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should contain theSameElementsInOrderAs Seq(
          LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(4)),
        )
    }

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(4)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should contain theSameElementsInOrderAs Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(4)),
        )
    }

    inside(
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(5)),
          LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(5)),
        ),
      ),
    ) {
      case newEntries =>
        newEntries should contain theSameElementsInOrderAs Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(5)),
          LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(5)),
        )
    }
  }

  it should "throw an IllegalArgumentException if the given entries conflict with a committed entry" in {
    val data = RaftMemberData(
      replicatedLog = ReplicatedLog()
        .reset(Term(2), LogEntryIndex(3))
        .truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(3)),
            LogEntry(LogEntryIndex(6), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(3)),
          ),
        ),
      commitIndex = LogEntryIndex(6),
    )

    val exception = intercept[IllegalArgumentException] {
      data.resolveNewLogEntries(
        Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(7), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(4)),
        ),
      )
    }
    exception.getMessage should be(
      "requirement failed: The entry with index [6] should not conflict with the committed entry (commitIndex [6])",
    )
  }

  behavior of "RaftMemberData.handleCommittedLogEntriesAndClients"

  it should "call the given handler with applicable log entries" in {
    // Arrange:
    val logEntryWithIndex2 =
      LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1))
    val logEntryWithIndex3 =
      LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1))
    val clientContextForIndex3 =
      ClientContext(ActorRef.noSender, Some(EntityInstanceId(1)), None)
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          logEntryWithIndex2,
          logEntryWithIndex3,
          LogEntry(LogEntryIndex(4), EntityEvent(Some(NormalizedEntityId("entity-3")), "event-c"), Term(1)),
        ),
      )
      .commit(LogEntryIndex(1))
      .applyCommittedLogEntries { entries =>
        // do nothing
      }
      .registerClient(clientContextForIndex3, LogEntryIndex(3))
      .commit(LogEntryIndex(3))
    data.commitIndex should be(LogEntryIndex(3))
    data.lastApplied should be(LogEntryIndex(1))

    // Act & Assert:
    val handlerMock = mockFunction[Seq[(LogEntry, Option[ClientContext])], Unit]
    val expectedApplicableLogEntries = Seq(
      (logEntryWithIndex2, None),
      (logEntryWithIndex3, Some(clientContextForIndex3)),
    )
    handlerMock.expects(expectedApplicableLogEntries).once()
    data.handleCommittedLogEntriesAndClients(handlerMock)
  }

  it should "call the given handler with empty applicable log entries" in {
    // Arrange:
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .commit(LogEntryIndex(2))
      .applyCommittedLogEntries { entries =>
        // do nothing
      }
    data.commitIndex should be(LogEntryIndex(2))
    data.lastApplied should be(LogEntryIndex(2))

    // Act & Assert:
    val handlerMock = mockFunction[Seq[(LogEntry, Option[ClientContext])], Unit]
    handlerMock.expects(Seq.empty).once()
    data.handleCommittedLogEntriesAndClients(handlerMock)
  }

  it should "update lastApplied to the last index of applicable log entries" in {
    // Arrange:
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(NormalizedEntityId("entity-3")), "event-c"), Term(1)),
        ),
      )
      .commit(LogEntryIndex(1))
      .applyCommittedLogEntries { entries =>
        // do nothing
      }
      .commit(LogEntryIndex(3))
    data.commitIndex should be(LogEntryIndex(3))
    data.lastApplied should be(LogEntryIndex(1))

    // Act & Assert:
    val handlerMock = mockFunction[Seq[(LogEntry, Option[ClientContext])], Unit]
    handlerMock
      .expects(where { (entries: Seq[(LogEntry, Option[ClientContext])]) =>
        entries.last._1.index === LogEntryIndex(3)
      }).once()
    val newData = data.handleCommittedLogEntriesAndClients(handlerMock)
    newData.lastApplied should be(LogEntryIndex(3))
  }

  it should "not update lastApplied if applicable log entries are empty" in {
    // Arrange:
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .commit(LogEntryIndex(2))
      .applyCommittedLogEntries { entries =>
        // do nothing
      }
    data.commitIndex should be(LogEntryIndex(2))
    data.lastApplied should be(LogEntryIndex(2))

    // Act & Assert:
    val handlerMock = mockFunction[Seq[(LogEntry, Option[ClientContext])], Unit]
    handlerMock.expects(Seq.empty).once()
    val newData = data.handleCommittedLogEntriesAndClients(handlerMock)
    newData.lastApplied should be(LogEntryIndex(2))
  }

  it should "remove clients only for applicable log entries" in {
    // Arrange:
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .commit(LogEntryIndex(1))
      .applyCommittedLogEntries { entries =>
        // do nothing
      }
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(1)), None), LogEntryIndex(2))
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(2)), None), LogEntryIndex(3))
      .commit(LogEntryIndex(2))
    data.commitIndex should be(LogEntryIndex(2))
    data.lastApplied should be(LogEntryIndex(1))
    data.clients.keySet should contain(LogEntryIndex(2))
    data.clients.keySet should contain(LogEntryIndex(3))

    // Act & Assert:
    val handlerMock = mockFunction[Seq[(LogEntry, Option[ClientContext])], Unit]
    handlerMock
      .expects(where { (entries: Seq[(LogEntry, Option[ClientContext])]) =>
        val indicesOfApplicableLogEntries = entries.map(_._1.index)
        indicesOfApplicableLogEntries.contains(LogEntryIndex(2)) &&
        !indicesOfApplicableLogEntries.contains(LogEntryIndex(3))
      }).once()
    val newData = data.handleCommittedLogEntriesAndClients(handlerMock)
    newData.clients.keySet shouldNot contain(LogEntryIndex(2))
    newData.clients.keySet should contain(LogEntryIndex(3))
  }

  behavior of "RaftMemberData.discardConflictClients"

  it should "discard no existing clients and call no onDiscard handler if the given index is None" in {
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(1)), None), LogEntryIndex(2))
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(2)), None), LogEntryIndex(3))

    var discardedClients = Set.empty[ClientContext]
    val newData          = data.discardConflictClients(None, conflictClient => discardedClients += conflictClient)

    newData.clients should be(data.clients)
    discardedClients should be(empty)
  }

  it should "discard no existing clients and call no onDiscard handler if the given index is greater than the last log index plus one" in {
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(1)), None), LogEntryIndex(2))
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(2)), None), LogEntryIndex(3))

    var discardedClients = Set.empty[ClientContext]
    val newData = data.discardConflictClients(
      Some(LogEntryIndex(5)),
      conflictClient => discardedClients += conflictClient,
    )

    newData.clients should be(data.clients)
    discardedClients should be(empty)
  }

  it should "discard no existing clients and call no onDiscard handler if the given index is equal to the last log index plus one" in {
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(1)), None), LogEntryIndex(2))
      .registerClient(ClientContext(ActorRef.noSender, Some(EntityInstanceId(2)), None), LogEntryIndex(3))

    var discardedClients = Set.empty[ClientContext]
    val newData = data.discardConflictClients(
      Some(LogEntryIndex(4)),
      conflictClient => discardedClients += conflictClient,
    )

    newData.clients should be(data.clients)
    discardedClients should be(empty)
  }

  it should "discard conflict clients and call onDiscard handler with each conflict clients if conflict clients exist" in {
    val client1 = ClientContext(ActorRef.noSender, Some(EntityInstanceId(1)), None)
    val client2 = ClientContext(ActorRef.noSender, Some(EntityInstanceId(2)), None)
    val data = RaftMemberData()
      .truncateAndAppendEntries(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
        ),
      )
      .registerClient(client1, LogEntryIndex(2))
      .registerClient(client2, LogEntryIndex(3))

    var discardedClients = Set.empty[ClientContext]
    val newData = data.discardConflictClients(
      Some(LogEntryIndex(2)),
      conflictClient => discardedClients += conflictClient,
    )

    assert(!newData.clients.contains(LogEntryIndex(2)))
    assert(!newData.clients.contains(LogEntryIndex(3)))
    assert(discardedClients.contains(client1))
    assert(discardedClients.contains(client2))
  }

  behavior of "RaftMemberData.followLeaderCommit"

  it should "not update commitIndex if the given leaderCommit is less than commitIndex" in {
    val replicatedLog = ReplicatedLog().truncateAndAppend(
      Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
      ),
    )
    val data = RaftMemberData(
      commitIndex = LogEntryIndex(3),
      replicatedLog = replicatedLog,
    )
    val newData = data.followLeaderCommit(LogEntryIndex(2))
    newData.commitIndex should be(LogEntryIndex(3))
  }

  it should "not update commitIndex if the given leaderCommit is equal to commitIndex" in {
    val replicatedLog = ReplicatedLog().truncateAndAppend(
      Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
      ),
    )
    val data = RaftMemberData(
      commitIndex = LogEntryIndex(3),
      replicatedLog = replicatedLog,
    )
    val newData = data.followLeaderCommit(LogEntryIndex(3))
    newData.commitIndex should be(LogEntryIndex(3))
  }

  it should "update commitIndex to leaderCommit if leaderCommit is greater than commitIndex and less than the last index of replicatedLog" in {
    val replicatedLog = ReplicatedLog().truncateAndAppend(
      Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
      ),
    )
    val data = RaftMemberData(
      commitIndex = LogEntryIndex(2),
      replicatedLog = replicatedLog,
    )
    val newData = data.followLeaderCommit(LogEntryIndex(3))
    newData.commitIndex should be(LogEntryIndex(3))
  }

  it should "update commitIndex to leaderCommit if leaderCommit is greater than commitIndex and equal to the last index of replicatedLog" in {
    val replicatedLog = ReplicatedLog().truncateAndAppend(
      Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
      ),
    )
    val data = RaftMemberData(
      commitIndex = LogEntryIndex(2),
      replicatedLog = replicatedLog,
    )
    val newData = data.followLeaderCommit(LogEntryIndex(4))
    newData.commitIndex should be(LogEntryIndex(4))
  }

  it should "throw an IllegalArgumentException if leaderCommit is greater than the last index of replicatedLog" in {
    val replicatedLog = ReplicatedLog().truncateAndAppend(
      Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(3)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
      ),
    )
    val data = RaftMemberData(
      commitIndex = LogEntryIndex(3),
      replicatedLog = replicatedLog,
    )
    val exception = intercept[IllegalArgumentException] {
      data.followLeaderCommit(LogEntryIndex(5))
    }
    exception.getMessage should be(
      "requirement failed: The given leaderCommit [5] should be less than or equal to ReplicatedLog.lastLogIndex [4]. " +
      "The caller should append entries with indices (from=[4], to=[5]) into ReplicatedLog before committing the given leaderCommit.",
    )
  }

  private def generateEntityId() = {
    NormalizedEntityId.from(UUID.randomUUID().toString)
  }

  private def assertEqualsLogEntry(expected: LogEntry, actual: LogEntry): Unit = {
    actual.term should be(expected.term)
    actual.index should be(expected.index)
    actual.event should be(expected.event)
  }

  private def nonDirtySnapshotStatus(snapshotLastTerm: Term, snapshotLastLogIndex: LogEntryIndex): SnapshotStatus = {
    SnapshotStatus(
      snapshotLastTerm = snapshotLastTerm,
      snapshotLastLogIndex = snapshotLastLogIndex,
      targetSnapshotLastTerm = snapshotLastTerm,
      targetSnapshotLastLogIndex = snapshotLastLogIndex,
    )
  }
}
