package lerna.akka.entityreplication.rollback

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.testkit.TestKitBase
import com.datastax.oss.driver.api.core.uuid.Uuids
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftActor
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntryIndex, NoOp, Term }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.rollback.setup.RaftActorId
import lerna.akka.entityreplication.rollback.testkit.{
  ConstantPersistenceQueries,
  PatienceConfigurationForTestKitBase,
  TimeBasedUuids,
}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import java.time.{ Instant, ZoneOffset, ZonedDateTime }

object RaftShardPersistenceQueriesSpec {

  private def nextWriterUuid(): String =
    Uuids.random().toString

  private def createEventEnvelopes(
      persistenceId: String,
      baseTimestamp: Instant,
      events: Seq[RaftActor.PersistEvent],
  ): IndexedSeq[PersistenceQueries.TaggedEventEnvelope] = {
    val timeBasedUuids = new TimeBasedUuids(baseTimestamp)
    val writerUuid     = nextWriterUuid()
    events.zipWithIndex.map {
      case (event, index) =>
        PersistenceQueries.TaggedEventEnvelope(
          persistenceId,
          SequenceNr(value = index + 1), // 1-based indexing
          event,
          timeBasedUuids.create(deltaMillis = index),
          Set.empty,
          writerUuid,
        )
    }.toIndexedSeq
  }

}

final class RaftShardPersistenceQueriesSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with PatienceConfigurationForTestKitBase {
  import RaftShardPersistenceQueriesSpec._

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  "RaftShardPersistenceQueries.entityIdsAfter" should {

    "return a Source that emits all involved entity IDs after the given RaftActor's sequence number inclusive" in {
      // Prepare:
      val raftActorId =
        RaftActorId(TypeName.from("example"), NormalizedShardId.from("1"), MemberIndex("replica-group-1"))
      val eventEnvelopes =
        createEventEnvelopes(
          raftActorId.persistenceId,
          ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
          Seq(
            RaftActor.BegunNewTerm(Term(1)),
            RaftActor.AppendedEvent(EntityEvent(None, NoOp)),
            RaftActor.AppendedEvent(EntityEvent(Some(NormalizedEntityId("entity-a")), "event-a1")),
            RaftActor.AppendedEvent(EntityEvent(Some(NormalizedEntityId("entity-b")), "event-b1")),
            RaftActor.CompactionCompleted(
              raftActorId.memberIndex,
              raftActorId.shardId,
              Term(1),
              LogEntryIndex(4),
              Set(NormalizedEntityId("entity-a"), NormalizedEntityId("entity-b")),
            ),
            RaftActor.AppendedEvent(EntityEvent(Some(NormalizedEntityId("entity-a")), "event-a2")),
            RaftActor.AppendedEvent(EntityEvent(Some(NormalizedEntityId("entity-c")), "event-c1")),
            RaftActor.DetectedNewTerm(Term(2)),
          ),
        )
      val queries = new RaftShardPersistenceQueries(new ConstantPersistenceQueries(eventEnvelopes))

      // Test:
      queries.entityIdsAfter(raftActorId, SequenceNr(3)).runWith(Sink.seq).futureValue should be(
        Seq(
          NormalizedEntityId("entity-a"),
          NormalizedEntityId("entity-b"),
          NormalizedEntityId("entity-a"),
          NormalizedEntityId("entity-c"),
        ),
      )
      queries.entityIdsAfter(raftActorId, SequenceNr(5)).runWith(Sink.seq).futureValue should be(
        Seq(
          NormalizedEntityId("entity-a"),
          NormalizedEntityId("entity-c"),
        ),
      )
      queries.entityIdsAfter(raftActorId, SequenceNr(8)).runWith(Sink.seq).futureValue should be(empty)
    }

  }

  "RaftShardPersistenceQueries.findLastTruncatedLogEntryIndex" should {

    "return the last LogEntryIndex with which the given RaftActor has truncated its ReplicatedLog entries" in {
      // Prepare:
      val raftActorId =
        RaftActorId(TypeName.from("example"), NormalizedShardId.from("1"), MemberIndex("replica-group-1"))
      val eventEnvelopes =
        createEventEnvelopes(
          raftActorId.persistenceId,
          ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
          Seq(
            RaftActor.BegunNewTerm(Term(1)),
            RaftActor.DetectedNewTerm(Term(2)),
            RaftActor.SnapshotSyncStarted(Term(2), LogEntryIndex(10)),
            RaftActor.SnapshotSyncCompleted(Term(2), LogEntryIndex(10)),
            RaftActor.BegunNewTerm(Term(3)),
            RaftActor.AppendedEvent(EntityEvent(None, NoOp)),
            RaftActor.AppendedEvent(EntityEvent(Some(NormalizedEntityId("entity-a")), "event-a1")),
            RaftActor.CompactionCompleted(
              raftActorId.memberIndex,
              raftActorId.shardId,
              Term(3),
              LogEntryIndex(12),
              Set(NormalizedEntityId("entity-a")),
            ),
            RaftActor.DetectedNewTerm(Term(4)),
          ),
        )
      val queries = new RaftShardPersistenceQueries(new ConstantPersistenceQueries(eventEnvelopes))

      // Test:
      queries.findLastTruncatedLogEntryIndex(raftActorId, SequenceNr(9)).futureValue should be(Some(LogEntryIndex(12)))
      queries.findLastTruncatedLogEntryIndex(raftActorId, SequenceNr(8)).futureValue should be(Some(LogEntryIndex(12)))
      queries.findLastTruncatedLogEntryIndex(raftActorId, SequenceNr(7)).futureValue should be(Some(LogEntryIndex(10)))
      queries.findLastTruncatedLogEntryIndex(raftActorId, SequenceNr(4)).futureValue should be(Some(LogEntryIndex(10)))
      queries.findLastTruncatedLogEntryIndex(raftActorId, SequenceNr(3)).futureValue should be(None)
    }

  }

}
