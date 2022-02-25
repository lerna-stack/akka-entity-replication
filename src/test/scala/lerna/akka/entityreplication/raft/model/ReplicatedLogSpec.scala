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

    "return all logEntries by getFrom(LogEntryIndex, maxEntryCount, maxBatchCount) when (maxEntryCount * maxBatchCount) is greater value than count of logEntries" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val target   = LogEntryIndex(4)
      val expected = Seq(Seq(logEntries(3), logEntries(4))) // entries from LogEntryIndex(4) to last

      val log = new ReplicatedLog(logEntries)

      log.getFrom(target, maxEntryCount = 10, maxBatchCount = 1) should be(expected)
    }

    "return only one part of entries by getFrom(..., maxBatchCount=1) even if it has more succeeding entries" in {

      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(None, "f"), Term(1)),
      )
      val log = new ReplicatedLog(logEntries)

      val expectedParts = Seq(Seq(logEntries(2), logEntries(3)))
      log.getFrom(LogEntryIndex(3), maxEntryCount = 2, maxBatchCount = 1) shouldBe expectedParts

    }

    "return part of logEntries by getFrom(LogEntryIndex, maxEntryCount, maxBatchCount) when (maxEntryCount * maxBatchCount) is lower value than count of logEntries" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(None, "f"), Term(1)),
      )

      val target = LogEntryIndex(3)
      val expected =
        Seq(
          Seq(logEntries(2), logEntries(3)),
          Seq(logEntries(4), logEntries(5)),
        ) // 4 entries from LogEntryIndex(3)

      val log = new ReplicatedLog(logEntries)

      log.getFrom(target, maxEntryCount = 2, maxBatchCount = 2) should be(expected)
    }

    "return part of logEntries by sliceEntriesFromHead(to: LogEntryIndex) when entries are nonEmpty" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(None, "f"), Term(1)),
      )

      val to       = LogEntryIndex(5)
      val expected = Seq(1, 2, 3, 4, 5)

      val log = new ReplicatedLog(logEntries)

      log.sliceEntriesFromHead(to = to).map(_.index.underlying) should be(expected)
    }

    "return Seq() by sliceEntriesFromHead(to: LogEntryIndex) when entries are empty" in {
      val logEntries = Seq()

      val to       = LogEntryIndex(5)
      val expected = Seq()

      val log = new ReplicatedLog(logEntries)

      log.sliceEntriesFromHead(to = to) should be(expected)
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

    "return the term corresponding to the LogEntryIndex if termAt is called" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(3)),
      )
      val log = new ReplicatedLog(logEntries)

      log.termAt(LogEntryIndex(3)) should contain(Term(3))
    }

    "return the initial term if termAt is passed the initial LogEntryIndex" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(3)),
      )
      val log = new ReplicatedLog(logEntries)

      log.termAt(LogEntryIndex.initial()) should contain(Term.initial())
    }

    "return the ancestorLastTerm if termAt is passed ancestorLastIndex after reset" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(3)),
      )
      val log = new ReplicatedLog(logEntries).reset(ancestorLastTerm = Term(4), ancestorLastIndex = LogEntryIndex(10))

      log.termAt(LogEntryIndex(10)) should contain(Term(4))
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

  "ReplicatedLog.isGivenLogUpToDate" should {

    "return true if term > lastLogTerm" in {

      val empty = ReplicatedLog()
      assert(empty.isGivenLogUpToDate(Term(1), LogEntryIndex(0))) // index = lastLogIndex
      assert(empty.isGivenLogUpToDate(Term(1), LogEntryIndex(1))) // index > lastLogIndex

      val emptyWithAncestor = ReplicatedLog().reset(Term(3), LogEntryIndex(7))
      assert(emptyWithAncestor.isGivenLogUpToDate(Term(4), LogEntryIndex(6))) // index < lastLogIndex
      assert(emptyWithAncestor.isGivenLogUpToDate(Term(4), LogEntryIndex(7))) // index = lastLogIndex
      assert(emptyWithAncestor.isGivenLogUpToDate(Term(4), LogEntryIndex(8))) // index > lastLogIndex

      val nonEmpty = ReplicatedLog().merge(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        ),
        LogEntryIndex(0),
      )
      assert(nonEmpty.isGivenLogUpToDate(Term(3), LogEntryIndex(2))) // index < lastLogIndex
      assert(nonEmpty.isGivenLogUpToDate(Term(3), LogEntryIndex(3))) // index = lastLogIndex
      assert(nonEmpty.isGivenLogUpToDate(Term(3), LogEntryIndex(4))) // index > lastLogIndex

    }

    "return true if term = lastLogTerm & index >= lastLogIndex" in {

      val empty = ReplicatedLog()
      assert(empty.isGivenLogUpToDate(Term(0), LogEntryIndex(0))) // index = lastLogIndex
      assert(empty.isGivenLogUpToDate(Term(0), LogEntryIndex(1))) // index > lastLogIndex

      val emptyWithAncestor = ReplicatedLog().reset(Term(3), LogEntryIndex(7))
      assert(emptyWithAncestor.isGivenLogUpToDate(Term(3), LogEntryIndex(7))) // index = lastLogIndex
      assert(emptyWithAncestor.isGivenLogUpToDate(Term(3), LogEntryIndex(8))) // index > lastLogIndex

      val nonEmpty = ReplicatedLog().merge(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        ),
        LogEntryIndex(0),
      )
      assert(nonEmpty.isGivenLogUpToDate(Term(2), LogEntryIndex(3))) // index = lastLogIndex
      assert(nonEmpty.isGivenLogUpToDate(Term(2), LogEntryIndex(4))) // index > lastLogIndex

    }

    "return false if term = lastLogTerm & index < lastLogIndex" in {

      val emptyWithAncestor = ReplicatedLog().reset(Term(3), LogEntryIndex(7))
      assert(!emptyWithAncestor.isGivenLogUpToDate(Term(3), LogEntryIndex(6))) // index < lastLogIndex

      val nonEmpty = ReplicatedLog().merge(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        ),
        LogEntryIndex(0),
      )
      assert(!nonEmpty.isGivenLogUpToDate(Term(2), LogEntryIndex(2))) // index < lastLogIndex

    }

    "return false if term < lastLogTerm" in {

      val emptyWithAncestor = ReplicatedLog().reset(Term(3), LogEntryIndex(7))
      assert(!emptyWithAncestor.isGivenLogUpToDate(Term(2), LogEntryIndex(6))) // index < lastLogIndex
      assert(!emptyWithAncestor.isGivenLogUpToDate(Term(2), LogEntryIndex(7))) // index = lastLogIndex
      assert(!emptyWithAncestor.isGivenLogUpToDate(Term(2), LogEntryIndex(8))) // index > lastLogIndex

      val nonEmpty = ReplicatedLog().merge(
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        ),
        LogEntryIndex(0),
      )
      assert(!nonEmpty.isGivenLogUpToDate(Term(1), LogEntryIndex(2))) // index < lastLogIndex
      assert(!nonEmpty.isGivenLogUpToDate(Term(1), LogEntryIndex(3))) // index = lastLogIndex
      assert(!nonEmpty.isGivenLogUpToDate(Term(1), LogEntryIndex(4))) // index > lastLogIndex

    }

  }

  "ReplicatedLog.entriesAfter" should {

    "returns entries with deleted up to the specified LogEntryIndex" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
      )

      val log = new ReplicatedLog(logEntries)

      log.entriesAfter(index = LogEntryIndex.initial()).map(_.event.event).toList shouldBe List("a", "b", "c")
      log.entriesAfter(index = LogEntryIndex(1)).map(_.event.event).toList shouldBe List("b", "c")
      log.entriesAfter(index = LogEntryIndex(2)).map(_.event.event).toList shouldBe List("c")
      log.entriesAfter(index = LogEntryIndex(3)).map(_.event.event).toList shouldBe List()
      log.entriesAfter(index = LogEntryIndex(4)).map(_.event.event).toList shouldBe List()
    }

    "returns entries with deleted up to the specified LogEntryIndex when the log is compressed" in {
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(None, "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(None, "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(None, "e"), Term(1)),
      )

      val log = new ReplicatedLog(logEntries).deleteOldEntries(to = LogEntryIndex(2), preserveLogSize = 3)
      require(log.entries.map(_.index.underlying) == List(3, 4, 5))
      require(log.entries.map(_.event.event) == List("c", "d", "e"))

      log.entriesAfter(index = LogEntryIndex.initial()).map(_.event.event).toList shouldBe List("c", "d", "e")
      log.entriesAfter(index = LogEntryIndex(1)).map(_.event.event).toList shouldBe List("c", "d", "e")
      log.entriesAfter(index = LogEntryIndex(2)).map(_.event.event).toList shouldBe List("c", "d", "e")
      log.entriesAfter(index = LogEntryIndex(3)).map(_.event.event).toList shouldBe List("d", "e")
      log.entriesAfter(index = LogEntryIndex(4)).map(_.event.event).toList shouldBe List("e")
      log.entriesAfter(index = LogEntryIndex(5)).map(_.event.event).toList shouldBe List()
      log.entriesAfter(index = LogEntryIndex(6)).map(_.event.event).toList shouldBe List()
    }

  }

}
