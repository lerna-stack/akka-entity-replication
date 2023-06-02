package lerna.akka.entityreplication.rollback

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.testkit.TestKitBase
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope
import lerna.akka.entityreplication.rollback.testkit.{ ConstantPersistenceQueries, PatienceConfigurationForTestKitBase }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.scalatest.concurrent.ScalaFutures

import java.time.{ Instant, ZoneOffset, ZonedDateTime }
import java.util.UUID

final class LinearRollbackTimestampHintFinderSpec
    extends TestKitBase
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with PatienceConfigurationForTestKitBase {

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  override def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }

  private trait Fixture {

    val persistenceId: String = "pid"
    val writerUUID: String    = UUID.randomUUID().toString

    def createEventEnvelope(sequenceNr: SequenceNr, timestamp: Instant): TaggedEventEnvelope =
      TaggedEventEnvelope(
        persistenceId,
        sequenceNr,
        s"event-${sequenceNr.value}",
        Offset.sequence(sequenceNr.value),
        timestamp.toEpochMilli,
        Set.empty,
        writerUUID,
      )

  }

  "LinearRollbackTimestampHintFinder.findTimestampHint" should {

    "return a rollback timestamp hint if an event with the given required lowest sequence number exists" in new Fixture {

      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = IndexedSeq(
        createEventEnvelope(SequenceNr(9), baseTimestamp.plusMillis(90)),
        createEventEnvelope(SequenceNr(10), baseTimestamp.plusMillis(100)),
        createEventEnvelope(SequenceNr(11), baseTimestamp.plusMillis(110)),
      )
      val queries = new ConstantPersistenceQueries(events)
      val finder  = new LinearRollbackTimestampHintFinder(system, queries)

      val requirements  = PersistentActorRollback.RollbackRequirements(persistenceId, SequenceNr(10))
      val timestampHint = finder.findTimestampHint(requirements).futureValue
      timestampHint.persistenceId should be(persistenceId)
      timestampHint.sequenceNr should be(SequenceNr(10))
      timestampHint.timestamp should be(baseTimestamp.plusMillis(100))
    }

    "return a rollback timestamp hint if an event with a sequence number higher than the given lowest one exists" in new Fixture {

      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = IndexedSeq(
        createEventEnvelope(SequenceNr(11), baseTimestamp.plusMillis(110)),
        createEventEnvelope(SequenceNr(12), baseTimestamp.plusMillis(120)),
      )
      val queries = new ConstantPersistenceQueries(events)
      val finder  = new LinearRollbackTimestampHintFinder(system, queries)

      val requirements  = PersistentActorRollback.RollbackRequirements(persistenceId, SequenceNr(10))
      val timestampHint = finder.findTimestampHint(requirements).futureValue
      timestampHint.persistenceId should be(persistenceId)
      timestampHint.sequenceNr should be(SequenceNr(11))
      timestampHint.timestamp should be(baseTimestamp.plusMillis(110))
    }

    "return a failed Future containing RollbackTimestampHintNotFound" +
    " if any event with a sequence number higher than or equal to the lowest one doesn't exist" in new Fixture {

      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = IndexedSeq(
        createEventEnvelope(SequenceNr(9), baseTimestamp.plusMillis(90)),
      )
      val queries = new ConstantPersistenceQueries(events)
      val finder  = new LinearRollbackTimestampHintFinder(system, queries)

      val requirements = PersistentActorRollback.RollbackRequirements(persistenceId, SequenceNr(10))
      val exception    = finder.findTimestampHint(requirements).failed.futureValue
      exception should be(a[RollbackTimestampHintNotFound])
      exception.getMessage should be(
        "Rollback timestamp hint not found:" +
        s" no events of persistenceId=[$persistenceId] with a sequence number greater than or equal to lowestSequenceNr=[10]",
      )
    }

  }

}
