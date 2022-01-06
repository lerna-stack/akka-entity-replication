package lerna.akka.entityreplication.util

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import lerna.akka.entityreplication.ClusterReplicationSettings

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

  /**
    * Persists events in specified order.
    * This operation blocks the calling thread until persistence is completed.
    */
  def persistEvents(events: Any*): Unit = {
    eventStore ! EventStore.PersistEvents(events)
    expectMsg(Done)
  }
}
