package lerna.akka.entityreplication.raft.protocol

import lerna.akka.entityreplication.model.NormalizedShardId
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, NoOp, Term }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Matchers, WordSpec }

final class RaftCommandsAppendEntriesSpec extends WordSpec with Matchers {

  import RaftCommands.AppendEntries

  "AppendEntries.committableIndex" should {

    "be leaderCommit if entries are empty" in {
      val appendEntries = AppendEntries(
        shardId = NormalizedShardId("shard-1"),
        term = Term(3),
        leader = MemberIndex("member-1"),
        prevLogIndex = LogEntryIndex(10),
        prevLogTerm = Term(3),
        entries = Seq.empty,
        leaderCommit = LogEntryIndex(10),
      )
      appendEntries.committableIndex should be(LogEntryIndex(10))
    }

    "be leaderCommit if leaderCommit is less than the first index of entries" in {
      val appendEntries = AppendEntries(
        shardId = NormalizedShardId("shard-1"),
        term = Term(3),
        leader = MemberIndex("member-1"),
        prevLogIndex = LogEntryIndex(5),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(3)),
        ),
        leaderCommit = LogEntryIndex(5),
      )
      appendEntries.committableIndex should be(LogEntryIndex(5))
    }

    "be leaderCommit if leaderCommit is equal to the first index of entries" in {
      val appendEntries = AppendEntries(
        shardId = NormalizedShardId("shard-1"),
        term = Term(5),
        leader = MemberIndex("member-1"),
        prevLogIndex = LogEntryIndex(5),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(8), EntityEvent(None, NoOp), Term(5)),
        ),
        leaderCommit = LogEntryIndex(6),
      )
      appendEntries.committableIndex should be(LogEntryIndex(6))
    }

    "be leaderCommit if leaderCommit is between the first and last index of entries" in {
      val appendEntries = AppendEntries(
        shardId = NormalizedShardId("shard-1"),
        term = Term(5),
        leader = MemberIndex("member-1"),
        prevLogIndex = LogEntryIndex(5),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(8), EntityEvent(None, NoOp), Term(5)),
        ),
        leaderCommit = LogEntryIndex(7),
      )
      appendEntries.committableIndex should be(LogEntryIndex(7))
    }

    "be leaderCommit if leaderCommit is equal to the last index of entries" in {
      val appendEntries = AppendEntries(
        shardId = NormalizedShardId("shard-1"),
        term = Term(5),
        leader = MemberIndex("member-1"),
        prevLogIndex = LogEntryIndex(5),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(8), EntityEvent(None, NoOp), Term(5)),
        ),
        leaderCommit = LogEntryIndex(8),
      )
      appendEntries.committableIndex should be(LogEntryIndex(8))
    }

    "be the last index of entries if the leaderCommit is greater than the last index of entries" in {
      val appendEntries = AppendEntries(
        shardId = NormalizedShardId("shard-1"),
        term = Term(5),
        leader = MemberIndex("member-1"),
        prevLogIndex = LogEntryIndex(5),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(6), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(7), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(8), EntityEvent(None, NoOp), Term(5)),
        ),
        leaderCommit = LogEntryIndex(9),
      )
      appendEntries.committableIndex should be(LogEntryIndex(8))
    }

  }

}
