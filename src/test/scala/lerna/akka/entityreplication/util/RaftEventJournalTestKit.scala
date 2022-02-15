package lerna.akka.entityreplication.util

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.query.{ EventEnvelope, PersistenceQuery }
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ ImplicitSender, TestKit }
import lerna.akka.entityreplication.ClusterReplicationSettings

import scala.concurrent.Await
import scala.reflect.ClassTag

/**
  * A TestKit for persisting events related Raft to the journal for testing purposes.
  */
object RaftEventJournalTestKit {

  def apply(system: ActorSystem, settings: ClusterReplicationSettings): RaftEventJournalTestKit =
    new RaftEventJournalTestKit(system, settings)
}

final class RaftEventJournalTestKit(system: ActorSystem, settings: ClusterReplicationSettings)
    extends TestKit(system)
    with ImplicitSender {

  private val eventStore = system.actorOf(EventStore.props(settings), "RaftEventPersistenceTestKitEventStore")

  private val readJournal =
    PersistenceQuery(system).readJournalFor[CurrentEventsByPersistenceIdQuery](settings.raftSettings.queryPluginId)

  /**
    * Persists events in specified order.
    * This operation blocks the calling thread until persistence is completed.
    */
  def persistEvents(events: Any*): Unit = {
    eventStore ! EventStore.PersistEvents(events)
    expectMsg(Done)
  }

  @volatile
  private var nextSeqNoByPersistenceId: Map[String, Long] = Map.empty

  private def nextSeqNo(persistenceId: String): Long =
    nextSeqNoByPersistenceId.getOrElse(persistenceId, 0)

  private def setNextSeqNo(envelope: EventEnvelope): Unit =
    nextSeqNoByPersistenceId = nextSeqNoByPersistenceId.updated(envelope.persistenceId, envelope.sequenceNr + 1)

  implicit val materializer: Materializer = Materializer(system)

  /**
    * Receive for max time next n events that have been persisted in the journal.
    */
  def receivePersisted[T](persistenceId: String, n: Int)(implicit tag: ClassTag[T]): Seq[T] = {
    require(n > 0, s"argument 'n'[${n}] should be greater than zero")
    val query =
      readJournal
        .currentEventsByPersistenceId(
          persistenceId,
          fromSequenceNr = nextSeqNo(persistenceId),
          toSequenceNr = Long.MaxValue,
        )
        .take(n)
        .runWith(Sink.seq[EventEnvelope])
    val result   = Await.result(query, remainingOrDefault)
    val filtered = result.map(_.event).filterNot(e => tag.runtimeClass.isInstance(e))
    assert(result.sizeIs == n, s"Could read only ${result.size} events instead of expected $n")
    assert(filtered.isEmpty, s"Persisted events [${filtered.mkString(", ")}] do not correspond to expected type")
    setNextSeqNo(result.last)
    result.map(_.event.asInstanceOf[T])
  }

  def reset(): Unit = {
    nextSeqNoByPersistenceId = Map.empty
  }
}
