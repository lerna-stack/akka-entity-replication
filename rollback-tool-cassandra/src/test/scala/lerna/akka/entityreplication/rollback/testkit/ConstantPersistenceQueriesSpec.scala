package lerna.akka.entityreplication.rollback.testkit

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.stream.scaladsl.Sink
import akka.testkit.TestKitBase
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope
import lerna.akka.entityreplication.rollback.SequenceNr
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.time.{ Instant, ZoneOffset, ZonedDateTime }
import java.util.UUID

final class ConstantPersistenceQueriesSpec
    extends TestKitBase
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures {

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  private val customPatienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(testKitSettings.SingleExpectDefaultTimeout),
    interval = scaled(testKitSettings.SingleExpectDefaultTimeout / 10),
  )
  override implicit def patienceConfig: PatienceConfig = customPatienceConfig

  override def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }

  private trait Fixture {
    val writerUuid: String     = UUID.randomUUID().toString
    val baseTimestamp: Instant = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

    def createEventEnvelope(
        persistenceId: String,
        sequenceNr: SequenceNr,
    ): TaggedEventEnvelope = {
      TaggedEventEnvelope(
        persistenceId,
        sequenceNr,
        s"event-$persistenceId-${sequenceNr.value}",
        Offset.sequence(sequenceNr.value),
        baseTimestamp.plusMillis(sequenceNr.value).toEpochMilli,
        Set.empty,
        writerUuid,
      )
    }
  }

  "ConstantPersistenceQueries" should {

    "throw no exception if the given event envelopes are empty" in {
      new ConstantPersistenceQueries(IndexedSeq.empty)
    }

    "throw an IllegalArgumentException if event envelopes have a different persistence ID" in new Fixture {
      val eventEnvelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
        createEventEnvelope("pid1", SequenceNr(2)),
        createEventEnvelope("pid2", SequenceNr(3)),
      )
      val exception = intercept[IllegalArgumentException] {
        new ConstantPersistenceQueries(eventEnvelopes)
      }
      exception.getMessage should be(
        "requirement failed: All event envelopes should have the same persistence ID, but they didn't. " +
        "Persistence IDs were [Set(pid1, pid2)].",
      )
    }

    "throw an IllegalArgumentException if an event envelope's sequence number is less than the previous one" in new Fixture {
      val eventEnvelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
        createEventEnvelope("pid1", SequenceNr(2)),
        createEventEnvelope("pid1", SequenceNr(1)),
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

    "return the highest sequence number after the given sequence number inclusive" in new Fixture {
      val eventEnvelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
        createEventEnvelope("pid1", SequenceNr(2)),
        createEventEnvelope("pid1", SequenceNr(3)),
        createEventEnvelope("pid1", SequenceNr(4)),
        createEventEnvelope("pid1", SequenceNr(5)),
      )
      val queries = new ConstantPersistenceQueries(eventEnvelopes)

      // Test:
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(1)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(2)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(3)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(4)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(5)).futureValue should be(Some(SequenceNr(5)))
      queries.findHighestSequenceNrAfter("pid1", SequenceNr(6)).futureValue should be(None)
    }

    "return None if the source of queries doesn't contain any event envelope for the given persistence ID" in new Fixture {
      val eventEnvelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
      )
      val queries = new ConstantPersistenceQueries(eventEnvelopes)
      queries.findHighestSequenceNrAfter("pid2", SequenceNr(1)).futureValue should be(None)
    }

  }

  "ConstantPersistenceQueries.currentEventsAfter" should {

    "return a source that emits event envelopes after the given sequence number inclusive" in new Fixture {
      val eventEnvelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
        createEventEnvelope("pid1", SequenceNr(2)),
        createEventEnvelope("pid1", SequenceNr(3)),
      )
      val queries = new ConstantPersistenceQueries(eventEnvelopes)

      // Test:
      queries.currentEventsAfter("pid1", SequenceNr(1)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(1)),
          createEventEnvelope("pid1", SequenceNr(2)),
          createEventEnvelope("pid1", SequenceNr(3)),
        ),
      )
      queries.currentEventsAfter("pid1", SequenceNr(2)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(2)),
          createEventEnvelope("pid1", SequenceNr(3)),
        ),
      )
      queries.currentEventsAfter("pid1", SequenceNr(3)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(3)),
        ),
      )
      queries.currentEventsAfter("pid1", SequenceNr(4)).runWith(Sink.seq).futureValue should be(empty)
    }

    "return an empty source if the source of queries doesn't contain any event envelope for the given persistence ID" in new Fixture {
      val envelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
      )
      val queries = new ConstantPersistenceQueries(envelopes)
      queries.currentEventsAfter("pid2", SequenceNr(1)).runWith(Sink.seq).futureValue should be(empty)
    }

  }

  "ConstantPersistenceQueries.currentEventsBefore" should {

    "return a source that emits event envelopes before the given sequence number inclusive in the descending order of the sequence number" in new Fixture {
      val eventEnvelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
        createEventEnvelope("pid1", SequenceNr(2)),
        createEventEnvelope("pid1", SequenceNr(3)),
      )
      val queries = new ConstantPersistenceQueries(eventEnvelopes)

      // Test:
      queries.currentEventsBefore("pid1", SequenceNr(1)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(1)),
        ),
      )
      queries.currentEventsBefore("pid1", SequenceNr(2)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(2)),
          createEventEnvelope("pid1", SequenceNr(1)),
        ),
      )
      queries.currentEventsBefore("pid1", SequenceNr(3)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(3)),
          createEventEnvelope("pid1", SequenceNr(2)),
          createEventEnvelope("pid1", SequenceNr(1)),
        ),
      )
      queries.currentEventsBefore("pid1", SequenceNr(4)).runWith(Sink.seq).futureValue should be(
        Seq(
          createEventEnvelope("pid1", SequenceNr(3)),
          createEventEnvelope("pid1", SequenceNr(2)),
          createEventEnvelope("pid1", SequenceNr(1)),
        ),
      )
    }

    "return an empty source if the source of queries doesn't contain any event envelope for the given persistence ID" in new Fixture {
      val envelopes = IndexedSeq(
        createEventEnvelope("pid1", SequenceNr(1)),
      )
      val queries = new ConstantPersistenceQueries(envelopes)
      queries.currentEventsBefore("pid2", SequenceNr(4)).runWith(Sink.seq).futureValue should be(empty)
    }

  }

}
