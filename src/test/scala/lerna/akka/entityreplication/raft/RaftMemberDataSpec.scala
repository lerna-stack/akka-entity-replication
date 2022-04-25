package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model._
import org.scalatest.{ FlatSpec, Inside, Matchers }

import java.util.UUID

final class RaftMemberDataSpec extends FlatSpec with Matchers with Inside {

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
