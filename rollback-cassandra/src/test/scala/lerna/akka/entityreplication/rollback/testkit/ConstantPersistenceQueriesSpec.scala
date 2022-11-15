package lerna.akka.entityreplication.rollback.testkit

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.testkit.TestKitBase
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope
import lerna.akka.entityreplication.rollback.SequenceNr
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import java.time.{ ZoneOffset, ZonedDateTime }
import java.util.UUID

object ConstantPersistenceQueriesSpec {

  private def nextWriterUuid(): String =
    UUID.randomUUID().toString

}

final class ConstantPersistenceQueriesSpec extends TestKitBase with WordSpecLike with Matchers with ScalaFutures {
  import ConstantPersistenceQueriesSpec._

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  private val customPatienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(testKitSettings.SingleExpectDefaultTimeout),
    interval = scaled(testKitSettings.SingleExpectDefaultTimeout / 10),
  )
  override implicit def patienceConfig: PatienceConfig = customPatienceConfig

  "ConstantPersistenceQueries" should {

    "throw no exception if the given event envelopes are empty" in {
      new ConstantPersistenceQueries(IndexedSeq.empty)
    }

    "throw an IllegalArgumentException if event envelopes have a different persistence ID" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val eventEnvelopes = IndexedSeq(
        TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, nextWriterUuid()),
        TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, nextWriterUuid()),
        TaggedEventEnvelope("pid2", SequenceNr(3), "event1", timeBasedUuids.create(+3), Set.empty, nextWriterUuid()),
      )
      val exception = intercept[IllegalArgumentException] {
        new ConstantPersistenceQueries(eventEnvelopes)
      }
      exception.getMessage should be(
        "requirement failed: All event envelopes should have the same persistence ID, but they didn't. " +
        "Persistence IDs were [Set(pid1, pid2)].",
      )
    }

    "throw an IllegalArgumentException if an event envelope's sequence number is less than the previous one" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val eventEnvelopes = IndexedSeq(
        TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, nextWriterUuid()),
        TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, nextWriterUuid()),
        TaggedEventEnvelope("pid1", SequenceNr(1), "event3", timeBasedUuids.create(+3), Set.empty, nextWriterUuid()),
      )
      val exception = intercept[IllegalArgumentException] {
        new ConstantPersistenceQueries(eventEnvelopes)
      }
      exception.getMessage should be(
        "requirement failed: eventEnvelopes(2).sequenceNr [1] should be greater than or equal to eventEnvelopes(1).sequenceNr [2]",
      )
    }

  }

  "ConstantPersistenceQueries.findHighestSequenceNrAfter" should {

    "return the highest sequence number after the given sequence number inclusive" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val eventEnvelopes = {
        val writerUuid = nextWriterUuid()
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", timeBasedUuids.create(+2), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", timeBasedUuids.create(+3), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(4), "event4", timeBasedUuids.create(+4), Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(5), "event5", timeBasedUuids.create(+5), Set.empty, writerUuid),
        )
      }
      val queries = new ConstantPersistenceQueries(eventEnvelopes)

      // Test:
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(1)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(2)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(3)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(4)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(5)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(6)).futureValue should be(None)
    }

    "return None if the source of queries doesn't contain any event envelope for the given persistence ID" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val eventEnvelopes = IndexedSeq(
        TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, nextWriterUuid()),
      )
      val queries = new ConstantPersistenceQueries(eventEnvelopes)
      queries.findHighestSequenceNrAfter("pid2", SequenceNr(1)).futureValue should be(None)
    }

  }

  "ConstantPersistenceQueries.currentEventsAfter" should {

    "return a source that emits event envelopes after the given sequence number inclusive" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val writerUuid = nextWriterUuid()
      val offset1    = timeBasedUuids.create(+1)
      val offset2    = timeBasedUuids.create(+2)
      val offset3    = timeBasedUuids.create(+3)
      val eventEnvelopes = {
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
        )
      }
      val queries = new ConstantPersistenceQueries(eventEnvelopes)

      // Test:
      queries.currentEventsAfter("pid1", SequenceNr(1)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
        ),
      )
      queries.currentEventsAfter("pid1", SequenceNr(2)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
        ),
      )
      queries.currentEventsAfter("pid1", SequenceNr(3)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
        ),
      )
      queries.currentEventsAfter("pid1", SequenceNr(4)).runWith(Sink.seq).futureValue should be(empty)
    }

    "return an empty source if the source of queries doesn't contain any event envelope for the given persistence ID" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val envelopes = IndexedSeq(
        TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, nextWriterUuid()),
      )
      val queries = new ConstantPersistenceQueries(envelopes)
      queries.currentEventsAfter("pid2", SequenceNr(1)).runWith(Sink.seq).futureValue should be(empty)
    }

  }

  "ConstantPersistenceQueries.currentEventsBefore" should {

    "return a source that emits event envelopes before the given sequence number inclusive in the descending order of the sequence number" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val writerUuid = nextWriterUuid()
      val offset1    = timeBasedUuids.create(+1)
      val offset2    = timeBasedUuids.create(+2)
      val offset3    = timeBasedUuids.create(+3)
      val eventEnvelopes = {
        IndexedSeq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
        )
      }
      val queries = new ConstantPersistenceQueries(eventEnvelopes)

      // Test:
      queries.currentEventsBefore("pid1", SequenceNr(1)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
        ),
      )
      queries.currentEventsBefore("pid1", SequenceNr(2)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
        ),
      )
      queries.currentEventsBefore("pid1", SequenceNr(3)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
        ),
      )
      queries.currentEventsBefore("pid1", SequenceNr(4)).runWith(Sink.seq).futureValue should be(
        Seq(
          TaggedEventEnvelope("pid1", SequenceNr(3), "event3", offset3, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(2), "event2", offset2, Set.empty, writerUuid),
          TaggedEventEnvelope("pid1", SequenceNr(1), "event1", offset1, Set.empty, writerUuid),
        ),
      )
    }

    "return an empty source if the source of queries doesn't contain any event envelope for the given persistence ID" in {
      val timeBasedUuids =
        new TimeBasedUuids(ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant)
      val envelopes = IndexedSeq(
        TaggedEventEnvelope("pid1", SequenceNr(1), "event1", timeBasedUuids.create(+1), Set.empty, nextWriterUuid()),
      )
      val queries = new ConstantPersistenceQueries(envelopes)
      queries.currentEventsBefore("pid2", SequenceNr(4)).runWith(Sink.seq).futureValue should be(empty)
    }

  }

}
