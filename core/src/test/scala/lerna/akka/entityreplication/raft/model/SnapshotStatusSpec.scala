package lerna.akka.entityreplication.raft.model

import org.scalatest.{ FlatSpec, Matchers }

class SnapshotStatusSpec extends FlatSpec with Matchers {

  behavior of "SnapshotStatus"

  it should "return true on 'isDirty' when ('snapshotLastTerm', 'snapshotLastLogIndex') != ('targetSnapshotLastTerm', 'targetSnapshotLastLogIndex')" in {
    // snapshotLastTerm < targetSnapshotLastTerm, snapshotLastLogIndex < targetSnapshotLastLogIndex
    SnapshotStatus(
      snapshotLastTerm = Term(20),
      snapshotLastLogIndex = LogEntryIndex(100),
      targetSnapshotLastTerm = Term(21),
      targetSnapshotLastLogIndex = LogEntryIndex(101),
    ).isDirty should be(true)

    // snapshotLastTerm = targetSnapshotLastTerm, snapshotLastLogIndex < targetSnapshotLastLogIndex
    SnapshotStatus(
      snapshotLastTerm = Term(20),
      snapshotLastLogIndex = LogEntryIndex(100),
      targetSnapshotLastTerm = Term(20),
      targetSnapshotLastLogIndex = LogEntryIndex(101),
    ).isDirty should be(true)

    // snapshotLastTerm < targetSnapshotLastTerm, snapshotLastLogIndex = targetSnapshotLastLogIndex
    SnapshotStatus(
      snapshotLastTerm = Term(20),
      snapshotLastLogIndex = LogEntryIndex(100),
      targetSnapshotLastTerm = Term(21),
      targetSnapshotLastLogIndex = LogEntryIndex(100),
    ).isDirty should be(true)

    // snapshotLastTerm = targetSnapshotLastTerm, snapshotLastLogIndex = targetSnapshotLastLogIndex
    SnapshotStatus(
      snapshotLastTerm = Term(20),
      snapshotLastLogIndex = LogEntryIndex(100),
      targetSnapshotLastTerm = Term(20),
      targetSnapshotLastLogIndex = LogEntryIndex(100),
    ).isDirty should be(false)
  }

  it should "raise IllegalArgumentException when snapshotLastTerm > targetSnapshotLastTerm" in {
    val ex = intercept[IllegalArgumentException] {
      SnapshotStatus(
        snapshotLastTerm = Term(20),
        snapshotLastLogIndex = LogEntryIndex(100),
        targetSnapshotLastTerm = Term(19),
        targetSnapshotLastLogIndex = LogEntryIndex(101),
      ).isDirty should be(true)
    }
    ex.getMessage should be(
      "requirement failed: snapshotLastTerm[Term(20)] must not exceed targetSnapshotLastTerm[Term(19)] (snapshotLastLogIndex[100], targetSnapshotLastLogIndex[101])",
    )
  }

  it should "raise IllegalArgumentException when snapshotLastLogIndex > targetSnapshotLastLogIndex" in {
    val ex = intercept[IllegalArgumentException] {
      SnapshotStatus(
        snapshotLastTerm = Term(20),
        snapshotLastLogIndex = LogEntryIndex(100),
        targetSnapshotLastTerm = Term(20),
        targetSnapshotLastLogIndex = LogEntryIndex(99),
      ).isDirty should be(true)
    }
    ex.getMessage should be(
      "requirement failed: snapshotLastLogIndex[100] must not exceed targetSnapshotLastLogIndex[99] (snapshotLastTerm[Term(20)], targetSnapshotLastTerm[Term(20)])",
    )
  }
}
