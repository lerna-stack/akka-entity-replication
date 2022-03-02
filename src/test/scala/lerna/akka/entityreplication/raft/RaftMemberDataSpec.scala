package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model._
import org.scalatest.{ FlatSpec, Matchers }

import java.util.UUID

class RaftMemberDataSpec extends FlatSpec with Matchers {

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
      replicatedLog = ReplicatedLog().merge(logEntries, prevLogIndex = LogEntryIndex.initial()),
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
      replicatedLog = ReplicatedLog().merge(logEntries, prevLogIndex = LogEntryIndex.initial()),
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
      replicatedLog = ReplicatedLog().merge(logEntries, prevLogIndex = LogEntryIndex.initial()),
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

  private def generateEntityId() = {
    NormalizedEntityId.from(UUID.randomUUID().toString)
  }
}
