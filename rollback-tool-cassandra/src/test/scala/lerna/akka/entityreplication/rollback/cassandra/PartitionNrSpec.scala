package lerna.akka.entityreplication.rollback.cassandra

import lerna.akka.entityreplication.rollback.SequenceNr
import org.scalatest.{ Matchers, WordSpec }

final class PartitionNrSpec extends WordSpec with Matchers {

  "PartitionNr" should {

    "return a PartitionNr whose value is equal to the given one" in {
      PartitionNr(0).value should be(0)
      PartitionNr(1).value should be(1)
      PartitionNr(2).value should be(2)
    }

    "throw an IllegalArgumentException if the given value is negative" in {
      val exception = intercept[IllegalArgumentException] {
        PartitionNr(-1)
      }
      exception.getMessage should be("requirement failed: value [-1] should be greater than or equal to 0")
    }

  }

  "PartitionNr.fromSequenceNr" should {

    "return a PartitionNr that the given sequence number belongs to" in {
      // partition size = 10
      PartitionNr.fromSequenceNr(SequenceNr(1), partitionSize = 10) should be(PartitionNr(0))
      PartitionNr.fromSequenceNr(SequenceNr(10), partitionSize = 10) should be(PartitionNr(0))
      PartitionNr.fromSequenceNr(SequenceNr(11), partitionSize = 10) should be(PartitionNr(1))
      PartitionNr.fromSequenceNr(SequenceNr(20), partitionSize = 10) should be(PartitionNr(1))
      PartitionNr.fromSequenceNr(SequenceNr(21), partitionSize = 10) should be(PartitionNr(2))
      // partition size = 500_000
      PartitionNr.fromSequenceNr(SequenceNr(1), partitionSize = 500_000) should be(PartitionNr(0))
      PartitionNr.fromSequenceNr(SequenceNr(500_000), partitionSize = 500_000) should be(PartitionNr(0))
      PartitionNr.fromSequenceNr(SequenceNr(500_001), partitionSize = 500_000) should be(PartitionNr(1))
      PartitionNr.fromSequenceNr(SequenceNr(1_000_000), partitionSize = 500_000) should be(PartitionNr(1))
      PartitionNr.fromSequenceNr(SequenceNr(1_000_001), partitionSize = 500_000) should be(PartitionNr(2))
    }

    "throw an IllegalArgumentException if the given partition size is 0" in {
      val exception = intercept[IllegalArgumentException] {
        PartitionNr.fromSequenceNr(SequenceNr(1), partitionSize = 0)
      }
      exception.getMessage should be("requirement failed: partitionSize [0] should be greater than 0")
    }

    "throw an IllegalArgumentException if the given partition size is negative" in {
      val exception = intercept[IllegalArgumentException] {
        PartitionNr.fromSequenceNr(SequenceNr(1), partitionSize = -1)
      }
      exception.getMessage should be("requirement failed: partitionSize [-1] should be greater than 0")
    }

  }

  "PartitionNr.+" should {

    "return a PartitionNr whose value is incremented by the given delta" in {
      (PartitionNr(2) + 1) should be(PartitionNr(3))
      (PartitionNr(2) + 2) should be(PartitionNr(4))
    }

    "throw an IllegalArgumentException if the new value is negative" in {
      val exception = intercept[IllegalArgumentException] {
        PartitionNr(Long.MaxValue) + 1
      }
      exception.getMessage should be(
        s"requirement failed: value [${Long.MinValue}] should be greater than or equal to 0",
      )
    }

  }

  "PartitionNr.compare" should {

    "return a negative integer if this PartitionNr is less than another PartitionNr" in {
      PartitionNr(0).compare(PartitionNr(1)) should be < 0
      PartitionNr(0).compare(PartitionNr(2)) should be < 0
      PartitionNr(0).compare(PartitionNr(3)) should be < 0
      PartitionNr(1).compare(PartitionNr(2)) should be < 0
      PartitionNr(1).compare(PartitionNr(3)) should be < 0
      PartitionNr(1).compare(PartitionNr(4)) should be < 0
    }

    "return zero if this PartitionNr is equal to another PartitionNr" in {
      PartitionNr(0).compare(PartitionNr(0)) should be(0)
      PartitionNr(1).compare(PartitionNr(1)) should be(0)
      PartitionNr(2).compare(PartitionNr(2)) should be(0)
    }

    "return a positive integer if PartitionNr is greater than another PartitionNr" in {
      PartitionNr(1).compare(PartitionNr(0)) should be > 0
      PartitionNr(2).compare(PartitionNr(1)) should be > 0
      PartitionNr(2).compare(PartitionNr(0)) should be > 0
      PartitionNr(3).compare(PartitionNr(2)) should be > 0
      PartitionNr(3).compare(PartitionNr(1)) should be > 0
      PartitionNr(3).compare(PartitionNr(0)) should be > 0
    }

  }

}
