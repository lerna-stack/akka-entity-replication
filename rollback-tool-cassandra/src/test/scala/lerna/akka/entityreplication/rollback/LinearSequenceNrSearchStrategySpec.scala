package lerna.akka.entityreplication.rollback

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.testkit.TestKitBase
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope
import lerna.akka.entityreplication.rollback.testkit.{ ConstantPersistenceQueries, PatienceConfigurationForTestKitBase }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.time.{ Instant, ZoneOffset, ZonedDateTime }
import java.util.UUID

final class LinearSequenceNrSearchStrategySpec
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
    val defaultWriterUuid: String = nextWriterUuid()

    def nextWriterUuid(): String =
      UUID.randomUUID().toString

    def createEventEnvelope(
        persistenceId: String,
        sequenceNr: SequenceNr,
        timestamp: Instant,
        writerUuid: String = defaultWriterUuid,
    ): TaggedEventEnvelope = {
      TaggedEventEnvelope(
        persistenceId,
        sequenceNr,
        s"event-$persistenceId-${sequenceNr.value}",
        Offset.sequence(sequenceNr.value),
        timestamp.toEpochMilli,
        Set.empty,
        writerUuid,
      )
    }
  }

  "LinearSequenceNrSearchStrategy.findUpperBound" should {

    "return None if the source is empty" in {
      val timestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val strategy  = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(IndexedSeq.empty))
      strategy.findUpperBound("pid1", timestamp).futureValue should be(None)
    }

    "return None if there is no event before the given timestamp inclusive" in new Fixture {
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1), baseTimestamp.plusMillis(+1)),
        createEventEnvelope("pid1", SequenceNr(2), baseTimestamp.plusMillis(+2)),
        createEventEnvelope("pid1", SequenceNr(3), baseTimestamp.plusMillis(+3)),
      )
      val strategy = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(events))
      strategy.findUpperBound("pid1", baseTimestamp).futureValue should be(None)
    }

    "return the highest sequence number of events before the given timestamp inclusive" in new Fixture {
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1), baseTimestamp.plusMillis(+1)),
        createEventEnvelope("pid1", SequenceNr(2), baseTimestamp.plusMillis(+2)),
        createEventEnvelope("pid1", SequenceNr(3), baseTimestamp.plusMillis(+3)),
      )
      val strategy = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(events))

      // Test:
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(1)).futureValue should be(Some(SequenceNr(1)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(2)).futureValue should be(Some(SequenceNr(2)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(3)).futureValue should be(Some(SequenceNr(3)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(4)).futureValue should be(Some(SequenceNr(3)))
    }

    "return None if the source contains no event envelopes for the given persistence ID" in new Fixture {
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1), baseTimestamp.plusMillis(+1)),
        createEventEnvelope("pid1", SequenceNr(2), baseTimestamp.plusMillis(+2)),
        createEventEnvelope("pid1", SequenceNr(3), baseTimestamp.plusMillis(+3)),
      )
      val strategy = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(events))
      strategy.findUpperBound("pid2", baseTimestamp.plusMillis(4)).futureValue should be(None)
    }

    "return the highest sequence number of events before the given timestamp inclusive if there is a clock out-of-sync" in new Fixture {
      // Suppose that writer B runs on a different Akka node other than the one writer A runs on.
      // Each Akka nodes has its own clock, which might be out of sync sometimes.
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val eventsFromWriterA = {
        val writerUuidA = nextWriterUuid()
        IndexedSeq(
          createEventEnvelope("pid1", SequenceNr(1), baseTimestamp.plusMillis(+1), writerUuidA),
          createEventEnvelope("pid1", SequenceNr(2), baseTimestamp.plusMillis(+2), writerUuidA),
          createEventEnvelope("pid1", SequenceNr(3), baseTimestamp.plusMillis(+3), writerUuidA),
        )
      }
      val eventsFromWriterB = {
        val writerUuidB = nextWriterUuid()
        IndexedSeq(
          createEventEnvelope("pid1", SequenceNr(4), baseTimestamp.plusMillis(-1), writerUuidB),
          createEventEnvelope("pid1", SequenceNr(5), baseTimestamp.plusMillis(+0), writerUuidB),
          createEventEnvelope("pid1", SequenceNr(6), baseTimestamp.plusMillis(+1), writerUuidB),
          createEventEnvelope("pid1", SequenceNr(7), baseTimestamp.plusMillis(+2), writerUuidB),
          createEventEnvelope("pid1", SequenceNr(8), baseTimestamp.plusMillis(+3), writerUuidB),
          createEventEnvelope("pid1", SequenceNr(9), baseTimestamp.plusMillis(+4), writerUuidB),
        )
      }
      val strategy = new LinearSequenceNrSearchStrategy(
        system,
        new ConstantPersistenceQueries(eventsFromWriterA ++ eventsFromWriterB),
      )

      // Test:
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(+5)).futureValue should be(Some(SequenceNr(9)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(+4)).futureValue should be(Some(SequenceNr(9)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(+3)).futureValue should be(Some(SequenceNr(8)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(+2)).futureValue should be(Some(SequenceNr(7)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(+1)).futureValue should be(Some(SequenceNr(6)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(0)).futureValue should be(Some(SequenceNr(5)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(-1)).futureValue should be(Some(SequenceNr(4)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(-2)).futureValue should be(None)
    }

  }

}
