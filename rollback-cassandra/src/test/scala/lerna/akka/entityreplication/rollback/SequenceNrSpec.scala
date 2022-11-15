package lerna.akka.entityreplication.rollback

import org.scalatest.{ Matchers, WordSpec }

final class SequenceNrSpec extends WordSpec with Matchers {

  "SequenceNr" should {

    "return a SequenceNr whose value is equal to the given one" in {
      SequenceNr(1).value should be(1)
      SequenceNr(2).value should be(2)
      SequenceNr(3).value should be(3)
    }

    "throw an IllegalArgumentException if the given value is 0" in {
      val exception = intercept[IllegalArgumentException] {
        SequenceNr(0)
      }
      exception.getMessage should be("requirement failed: value [0] should be greater than 0")
    }

    "throw an IllegalArgumentException if the given value is negative" in {
      val exception = intercept[IllegalArgumentException] {
        SequenceNr(-1)
      }
      exception.getMessage should be("requirement failed: value [-1] should be greater than 0")
    }

  }

  "SequenceNr.+" should {

    "return a SequenceNr whose value is incremented by the given delta" in {
      (SequenceNr(2) + 1) should be(SequenceNr(3))
      (SequenceNr(2) + 2) should be(SequenceNr(4))
    }

    "throw an IllegalArgumentException if the new value is 0" in {
      val exception = intercept[IllegalArgumentException] {
        SequenceNr(1) + -1
      }
      exception.getMessage should be(
        s"requirement failed: value [0] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the new value is negative" in {
      val exception = intercept[IllegalArgumentException] {
        SequenceNr(Long.MaxValue) + 1
      }
      exception.getMessage should be(
        s"requirement failed: value [${Long.MinValue}] should be greater than 0",
      )
    }

  }

  "SequenceNr.compare" should {

    "return a negative integer if this SequenceNr is less than another SequenceNr" in {
      SequenceNr(1).compare(SequenceNr(2)) should be < 0
      SequenceNr(1).compare(SequenceNr(3)) should be < 0
      SequenceNr(1).compare(SequenceNr(4)) should be < 0
      SequenceNr(2).compare(SequenceNr(3)) should be < 0
      SequenceNr(2).compare(SequenceNr(4)) should be < 0
      SequenceNr(2).compare(SequenceNr(5)) should be < 0
    }

    "return zero if this SequenceNr is equal to another SequenceNr" in {
      SequenceNr(1).compare(SequenceNr(1)) should be(0)
      SequenceNr(2).compare(SequenceNr(2)) should be(0)
      SequenceNr(3).compare(SequenceNr(3)) should be(0)
    }

    "return a positive integer if this SequenceNr is greater than another SequenceNr" in {
      SequenceNr(2).compare(SequenceNr(1)) should be > 0
      SequenceNr(3).compare(SequenceNr(2)) should be > 0
      SequenceNr(3).compare(SequenceNr(1)) should be > 0
      SequenceNr(4).compare(SequenceNr(3)) should be > 0
      SequenceNr(4).compare(SequenceNr(2)) should be > 0
      SequenceNr(4).compare(SequenceNr(1)) should be > 0
    }

  }

}
