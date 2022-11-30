package lerna.akka.entityreplication.rollback.setup

import org.scalatest.{ Matchers, WordSpec }

final class RollbackSetupSpec extends WordSpec with Matchers {

  "RollbackSetup" should {

    "not throw an IllegalArgumentException if the given toSequenceNr is 0" in {
      RollbackSetup("persistence-id-1", 0)
    }

    "throw an IllegalArgumentException if the given toSequenceNr is negative" in {
      val exception = intercept[IllegalArgumentException] {
        RollbackSetup("persistence-id-1", -1)
      }
      exception.getMessage should be("requirement failed: toSequenceNr [-1] should be greater than or equal to 0")
    }

  }

}
