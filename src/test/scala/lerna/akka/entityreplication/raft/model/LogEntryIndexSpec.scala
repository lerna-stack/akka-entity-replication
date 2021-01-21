package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.raft.model.exception.SeqIndexOutOfBoundsException
import org.scalatest.{ Matchers, WordSpec }

class LogEntryIndexSpec extends WordSpec with Matchers {

  "LogEntryIndex" should {

    "compute an index of Seq from self and offset" in {
      val index  = LogEntryIndex(Int.MaxValue.toLong + 2)
      val offset = LogEntryIndex(1)

      index.toSeqIndex(offset) should be(Int.MaxValue)
    }

    "throw an exception if the index of Seq is out of bounds" in {
      val index  = LogEntryIndex(Int.MaxValue.toLong + 2)
      val offset = LogEntryIndex(0)

      val caught = intercept[SeqIndexOutOfBoundsException] {
        index.toSeqIndex(offset)
      }

      caught.self should be(index)
      caught.offset should be(offset)
    }
  }
}
