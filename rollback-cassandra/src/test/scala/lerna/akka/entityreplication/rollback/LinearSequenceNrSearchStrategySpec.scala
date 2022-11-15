package lerna.akka.entityreplication.rollback

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope
import lerna.akka.entityreplication.rollback.testkit.{
  ConstantPersistenceQueries,
  PatienceConfigurationForTestKitBase,
  TimeBasedUuids,
}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import java.time.{ ZoneOffset, ZonedDateTime }
import java.util.UUID

object LinearSequenceNrSearchStrategySpec {

  def nextWriterUuid(): String =
    UUID.randomUUID().toString

}

final class LinearSequenceNrSearchStrategySpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with PatienceConfigurationForTestKitBase {
  import LinearSequenceNrSearchStrategySpec._

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  "LinearSequenceNrSearchStrategy.findUpperBound" should {

    "return None if the source is empty" in {
      val timestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val strategy  = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(IndexedSeq.empty))
      strategy.findUpperBound("pid1", timestamp).futureValue should be(None)
    }

    "return None if there is no event before the given timestamp inclusive" in {
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = {
        val timeBasedUuids = new TimeBasedUuids(baseTimestamp)
        val writerUuid     = nextWriterUuid()
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", timeBasedUuids.create(+3), Set.empty, writerUuid),
        )
      }
      val strategy = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(events))
      strategy.findUpperBound("pid1", baseTimestamp).futureValue should be(None)
    }

    "return the highest sequence number of events before the given timestamp inclusive" in {
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = {
        val timeBasedUuids = new TimeBasedUuids(baseTimestamp)
        val writer         = nextWriterUuid()
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, writer),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, writer),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", timeBasedUuids.create(+3), Set.empty, writer),
        )
      }
      val strategy = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(events))

      // Test:
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(1)).futureValue should be(Some(SequenceNr(1)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(2)).futureValue should be(Some(SequenceNr(2)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(3)).futureValue should be(Some(SequenceNr(3)))
      strategy.findUpperBound("pid1", baseTimestamp.plusMillis(4)).futureValue should be(Some(SequenceNr(3)))
    }

    "return None if the source contains no event envelopes for the given persistence ID" in {
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val events = {
        val timeBasedUuids = new TimeBasedUuids(baseTimestamp)
        val writerUuid     = nextWriterUuid()
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", timeBasedUuids.create(+3), Set.empty, writerUuid),
        )
      }
      val strategy = new LinearSequenceNrSearchStrategy(system, new ConstantPersistenceQueries(events))
      strategy.findUpperBound("pid2", baseTimestamp.plusMillis(4)).futureValue should be(None)
    }

    "return the highest sequence number of events before the given timestamp inclusive if there is a clock out-of-sync" in {
      // Suppose that writer B runs on a different Akka node other than the one writer A runs on.
      // Each Akka nodes has its own clock, which might be out of sync sometimes.
      val baseTimestamp = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val eventsFromWriterA = {
        val timeBasedUuids = new TimeBasedUuids(baseTimestamp)
        val writerUuidA    = nextWriterUuid()
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, writerUuidA),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, writerUuidA),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", timeBasedUuids.create(+3), Set.empty, writerUuidA),
        )
      }
      val eventsFromWriterB = {
        val timeBasedUuids = new TimeBasedUuids(baseTimestamp)
        val writerUuidB    = nextWriterUuid()
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(4), "event4", timeBasedUuids.create(-1), Set.empty, writerUuidB),
          TaggedEventEnvelope("pid1", SequenceNr(5), "event5", timeBasedUuids.create(0), Set.empty, writerUuidB),
          TaggedEventEnvelope("pid1", SequenceNr(6), "event6", timeBasedUuids.create(+1), Set.empty, writerUuidB),
          TaggedEventEnvelope("pid1", SequenceNr(7), "event7", timeBasedUuids.create(+2), Set.empty, writerUuidB),
          TaggedEventEnvelope("pid1", SequenceNr(8), "event8", timeBasedUuids.create(+3), Set.empty, writerUuidB),
          TaggedEventEnvelope("pid1", SequenceNr(9), "event9", timeBasedUuids.create(+4), Set.empty, writerUuidB),
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
