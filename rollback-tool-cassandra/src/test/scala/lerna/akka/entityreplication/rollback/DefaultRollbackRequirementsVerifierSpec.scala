package lerna.akka.entityreplication.rollback

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import lerna.akka.entityreplication.rollback.PersistentActorRollback.RollbackRequirements
import lerna.akka.entityreplication.rollback.testkit.PatienceConfigurationForTestKitBase
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.time.{ ZoneOffset, ZonedDateTime }
import scala.concurrent.Future

final class DefaultRollbackRequirementsVerifierSpec
    extends TestKitBase
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with MockFactory
    with PatienceConfigurationForTestKitBase {

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  override def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }

  private trait Fixture {
    val rollback: PersistentActorRollback                = mock[PersistentActorRollback]
    val timestampHintFinder: RollbackTimestampHintFinder = mock[RollbackTimestampHintFinder]
  }

  "DefaultRollbackRequirementsVerifier.verify" should {

    "return a successful Future if toSequenceNr is greater than the required lowest sequence number" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(5))))
        .once()

      // Test:
      verifier.verify("pid", Some(SequenceNr(6)), None).futureValue should be(Done)
    }

    "return a successful Future if toSequenceNr equals the required lowest sequence number" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(5))))
        .once()

      // Test:
      verifier.verify("pid", Some(SequenceNr(5)), None).futureValue should be(Done)
    }

    "return a failed Future containing a RollbackRequirementsNotFulfilled with a timestamp hint" +
    " if toSequenceNr is less than the required lowest sequence number" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(5))))
        .once()

      // Expectations:
      val timestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      (timestampHintFinder.findTimestampHint _)
        .expects(RollbackRequirements("pid", SequenceNr(5)))
        .returns(
          Future.successful(RollbackTimestampHintFinder.TimestampHint("pid", SequenceNr(5), timestamp.plusMillis(1))),
        )

      // Test:
      val exception = verifier.verify("pid", Some(SequenceNr(4)), Some(timestamp)).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
      exception.getMessage should be(
        "Rollback requirements not fulfilled:" +
        s" Rollback to sequence number [4] for the persistent actor [pid] is impossible" +
        s" since the sequence number should be greater than or equal to [5]." +
        s" Hint: rollback timestamp [$timestamp] should be newer than timestamp [${timestamp.plusMillis(1)}] of sequence number [5], at least.",
      )
    }

    "return a failed Future containing a RollbackRequirementsNotFulfilled without a timestamp hint" +
    "if toSequenceNr is less than the required lowest sequence number and the finding of a timestamp hint fails" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(5))))
        .once()

      // Expectations:
      val timestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      (timestampHintFinder.findTimestampHint _)
        .expects(RollbackRequirements("pid", SequenceNr(5)))
        .returns(Future.failed(new RuntimeException("unexpected failure")))

      // Test:
      val exception = verifier.verify("pid", Some(SequenceNr(4)), Some(timestamp)).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
      exception.getMessage should be(
        "Rollback requirements not fulfilled:" +
        s" Rollback to sequence number [4] for the persistent actor [pid] is impossible" +
        s" since the sequence number should be greater than or equal to [5]." +
        s" Hint: not available due to [${classOf[RuntimeException].getCanonicalName}]: unexpected failure].",
      )
    }

    "return a failed Future containing a RollbackRequirementsNotFulfilled without a timestamp hint" +
    " if toSequenceNr is less than the required lowest sequence number and toTimestampHintOpt is None" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(5))))
        .once()

      // Test:
      val exception = verifier.verify("pid", Some(SequenceNr(4)), None).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
      exception.getMessage should be(
        "Rollback requirements not fulfilled:" +
        s" Rollback to sequence number [4] for the persistent actor [pid] is impossible" +
        s" since the sequence number should be greater than or equal to [5].",
      )
    }

    "return a successful Future if toSequenceNr is None and the required lowest sequence number is 1" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(1))))
        .once()

      // Test:
      verifier.verify("pid", None, None).futureValue should be(Done)
    }

    "return a failed Future containing a RollbackRequirementsNotFulfilled with a timestamp hint" +
    " if toSequenceNr is None and the required lowest sequence number is not 1" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(2))))
        .once()

      // Expectations:
      val timestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      (timestampHintFinder.findTimestampHint _)
        .expects(RollbackRequirements("pid", SequenceNr(2)))
        .returns(
          Future.successful(RollbackTimestampHintFinder.TimestampHint("pid", SequenceNr(2), timestamp.plusMillis(1))),
        )

      // Test:
      val exception = verifier.verify("pid", None, Some(timestamp)).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
      exception.getMessage should be(
        "Rollback requirements not fulfilled:" +
        " Deleting all data for the persistent actor [pid] is impossible" +
        " since already deleted events might contain required data for consistency with other persistent actors." +
        s" Hint: rollback timestamp [$timestamp] should be newer than timestamp [${timestamp.plusMillis(1)}] of sequence number [2], at least.",
      )
    }

    "return a failed Future containing a RollbackRequirementsNotFulfilled without a timestamp hint" +
    "if toSequenceNr is None and the finding of a timestamp hint fails" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(2))))
        .once()

      // Expectations:
      val timestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      (timestampHintFinder.findTimestampHint _)
        .expects(RollbackRequirements("pid", SequenceNr(2)))
        .returns(Future.failed(new RuntimeException("unexpected failure")))

      // Test:
      val exception = verifier.verify("pid", None, Some(timestamp)).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
      exception.getMessage should be(
        "Rollback requirements not fulfilled:" +
        " Deleting all data for the persistent actor [pid] is impossible" +
        " since already deleted events might contain required data for consistency with other persistent actors." +
        s" Hint: not available due to [${classOf[RuntimeException].getCanonicalName}]: unexpected failure].",
      )
    }

    "return a failed Future containing a RollbackRequirementsNotFulfilled without a timestamp hint" +
    " if toSequenceNr is None, the required lowest sequence number is not 1, and toTimestampHintOpt is None" in new Fixture {
      val verifier = new DefaultRollbackRequirementsVerifier(system, rollback, timestampHintFinder)

      // Expectations:
      (rollback.findRollbackRequirements _)
        .expects("pid")
        .returns(Future.successful(RollbackRequirements("pid", SequenceNr(2))))
        .once()

      // Test:
      val exception = verifier.verify("pid", None, None).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
      exception.getMessage should be(
        "Rollback requirements not fulfilled:" +
        " Deleting all data for the persistent actor [pid] is impossible" +
        " since already deleted events might contain required data for consistency with other persistent actors.",
      )
    }

  }

}
