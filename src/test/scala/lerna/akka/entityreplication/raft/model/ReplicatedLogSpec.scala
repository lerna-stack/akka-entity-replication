package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.model.NormalizedEntityId
import org.scalatest.{ Matchers, WordSpecLike }

class ReplicatedLogSpec extends WordSpecLike with Matchers {

  "ReplicatedLog" should {

    "get(LogEntryIndex)でログを取得できる（存在する場合のみSome）" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val log = new ReplicatedLog(logEntries)

      log.get(LogEntryIndex(0)).map(_.index) should be(None)
      log.get(LogEntryIndex(1)).map(_.index) should be(Option(LogEntryIndex(1)))

      log.get(LogEntryIndex(5)).map(_.index) should be(Option(LogEntryIndex(5)))
      log.get(LogEntryIndex(6)).map(_.index) should be(None)
    }

    "return all logEntries by getFrom(LogEntryIndex, maxCount) when maxCount is greater value than count of logEntries" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val target   = LogEntryIndex(4)
      val expected = Seq(4, 5) // entries from LogEntryIndex(4) to last

      val log = new ReplicatedLog(logEntries)

      log.getFrom(target, maxCount = 10).map(_.index.underlying) should be(expected)
    }

    "return part of logEntries by getFrom(LogEntryIndex, maxCount) when maxCount is lower value than count of logEntries" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val target   = LogEntryIndex(3)
      val expected = Seq(3, 4) // 2 entries from LogEntryIndex(3)

      val log = new ReplicatedLog(logEntries)

      log.getFrom(target, maxCount = 2).map(_.index.underlying) should be(expected)
    }

    "sliceEntries(from, to)でログを切り出せる" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(None, "f"), Term(1)),
      )

      val from     = LogEntryIndex(3)
      val to       = LogEntryIndex(5)
      val expected = Seq(3, 4, 5)

      val log = new ReplicatedLog(logEntries)

      log.sliceEntries(from = from, to = to).map(_.index.underlying) should be(expected)
    }

    "イベントを追加できる" in {
      val log = ReplicatedLog()

      val entityId = NormalizedEntityId.from("test-entity")
      case object SomeEvent
      val term = Term.initial()

      val updatedLog = log.append(EntityEvent(Option(entityId), SomeEvent), term)

      updatedLog.last should be(LogEntry(LogEntryIndex(1), EntityEvent(Some(entityId), SomeEvent), term))
    }

    "イベントログの prevLogIndex が一致する部分以降がマージされる（単純追加）" in {

      val followerLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
      )

      val appendEntries = Seq(
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
      )

      val expectedLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
      )

      val log = new ReplicatedLog(followerLog)

      val updatedLog = log.merge(appendEntries, prevLogIndex = LogEntryIndex(2))

      updatedLog.entries should be(expectedLog)
      updatedLog.entries.map(_.event) should contain theSameElementsInOrderAs expectedLog.map(_.event)
    }

    "イベントログの prevLogIndex が一致する部分以降がマージされる（Follower の方が進んでいる）" in {

      val followerLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
      )

      val appendEntries = Seq(
        LogEntry(LogEntryIndex(3), EntityEvent(None, "e"), Term(2)),
      )

      val expectedLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "e"), Term(2)),
      )

      val log = new ReplicatedLog(followerLog)

      val updatedLog = log.merge(appendEntries, prevLogIndex = LogEntryIndex(2))

      updatedLog.entries should be(expectedLog)
      updatedLog.entries.map(_.event) should contain theSameElementsInOrderAs expectedLog.map(_.event)
    }

    "イベントログの prevLogIndex が一致する部分以降がマージされる（一つも一致しない）" in {

      val followerLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(2)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(2)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(2)),
      )

      val appendEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "c"), Term(1)),
      )

      val expectedLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "c"), Term(1)),
      )

      val log = new ReplicatedLog(followerLog)

      val updatedLog = log.merge(appendEntries, prevLogIndex = LogEntryIndex(0))

      updatedLog.entries should be(expectedLog)
      updatedLog.entries.map(_.event) should contain theSameElementsInOrderAs expectedLog.map(_.event)
    }

    /**
      * [[lerna.akka.entityreplication.raft.protocol.RaftCommands.AppendEntries]] は成功したが、
      * [[lerna.akka.entityreplication.raft.protocol.RaftCommands.AppendEntriesSucceeded]] がリーダーに届かなったケース
      */
    "イベントログの prevLogIndex が一致する部分以降がマージされる（重複あり）" in {

      val followerLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
      )

      val appendEntries = Seq(
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
      )

      val expectedLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
      )

      val log = new ReplicatedLog(followerLog)

      val updatedLog = log.merge(appendEntries, prevLogIndex = LogEntryIndex(1))

      updatedLog.entries should be(expectedLog)
      updatedLog.entries.map(_.event) should contain theSameElementsInOrderAs expectedLog.map(_.event)
    }

    "イベントログを 削除できる" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val log = new ReplicatedLog(logEntries)

      log.deleteOldEntries(to = LogEntryIndex(4), preserveLogSize = 1).entries.map(_.index.underlying) should be(
        Seq(5),
      )
      log.deleteOldEntries(to = LogEntryIndex(3), preserveLogSize = 1).entries.map(_.index.underlying) should be(
        Seq(4, 5),
      )
    }

    "keep log entries by preserveLogSize when deleting" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val log = new ReplicatedLog(logEntries)

      log.deleteOldEntries(to = LogEntryIndex(5), preserveLogSize = 2).entries.map(_.index.underlying) should be(
        Seq(4, 5),
      )
      log.deleteOldEntries(to = LogEntryIndex(4), preserveLogSize = 2).entries.map(_.index.underlying) should be(
        Seq(4, 5),
      )
    }

    "clear log entries and update lastLogIndex and lastLogTerm" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
      )

      val log = new ReplicatedLog(logEntries)

      val ancestorLastTerm  = Term(10)
      val ancestorLastIndex = LogEntryIndex(10)
      val updated           = log.reset(ancestorLastTerm, ancestorLastIndex)

      updated.entries should have size 0
      updated.lastLogTerm should be(ancestorLastTerm)
      updated.lastLogIndex should be(ancestorLastIndex)
    }
  }
}
