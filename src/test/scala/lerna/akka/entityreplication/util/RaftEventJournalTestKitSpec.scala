package lerna.akka.entityreplication.util

import akka.actor.ActorSystem
import akka.actor.Status
import akka.testkit.{ ImplicitSender, TestKit }
import akka.cluster.Cluster
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, StorageExtension }
import akka.persistence.journal.{ EventAdapter, EventSeq, Tagged }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.internal.ClusterReplicationSettingsImpl
import lerna.akka.entityreplication.testkit.KryoSerializable
import org.scalatest.{ BeforeAndAfterEach, FlatSpecLike, Matchers }

object RaftEventJournalTestKitSpec {

  class TestEventAdapter extends EventAdapter {

    override def manifest(event: Any): String = "" // No need

    override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single(event)

    override def toJournal(event: Any): Any =
      event match {
        case event: TaggedEvent => Tagged(event, Set(TaggedEvent.tag))
        case other              => throw new IllegalArgumentException(s"unknown: ${other}")
      }
  }

  object TaggedEvent {
    val tag: String = TaggedEvent.getClass.getName
  }
  case class TaggedEvent() extends KryoSerializable
}

class RaftEventJournalTestKitSpec
    extends TestKit(ActorSystem("RaftSnapshotStoreTestKitSpec"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterEach
    with ImplicitSender {

  private val cluster = Cluster(system)

  private val config = ConfigFactory
    .parseString {
      """
      lerna.akka.entityreplication.raft.persistence.journal-plugin-additional {
        event-adapters {
          test-event-adapter = "lerna.akka.entityreplication.util.RaftEventJournalTestKitSpec$TestEventAdapter"
        }
        event-adapter-bindings {
          "lerna.akka.entityreplication.util.RaftEventJournalTestKitSpec$TaggedEvent" = test-event-adapter
        }
      }
    """
    }.withFallback(system.settings.config)

  private val settings = ClusterReplicationSettingsImpl(config, cluster.settings.Roles)

  private val raftEventJournalTestKit = RaftEventJournalTestKit(system, settings)

  private val storage = StorageExtension(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    raftEventJournalTestKit.reset()
    // NOTE: RaftEventJournalTestKit does not depend on specific journal-plugin
    storage.journalStorage ! InMemoryJournalStorage.ClearJournal
    expectMsgType[Status.Success]
  }

  behavior of "RaftEventJournalTestKit"

  it should "return 'n' events that have been persisted with the persistenceId on receivePersisted" in {
    raftEventJournalTestKit.persistEvents("event1", "event2", "event3")
    // returns only 'n' events
    val result1 =
      raftEventJournalTestKit.receivePersisted[String](EventStore.persistenceId(), 2)
    result1 should be(Seq("event1", "event2"))
    // We can get the next persisted events after "event2" with the following call
    val result2 =
      raftEventJournalTestKit.receivePersisted[String](EventStore.persistenceId(), 1)
    result2 should be(Seq("event3"))
  }

  it should "raise IllegalArgumentException when the argument 'n' is less than 1 on receivePersisted" in {
    val ex =
      intercept[IllegalArgumentException] {
        raftEventJournalTestKit.receivePersisted("test", 0)
      }
    ex.getMessage should be("requirement failed: argument 'n'[0] should be greater than zero")
  }

  it should "raise AssertionError when number of the persisted events is insufficient on receivePersisted" in {
    raftEventJournalTestKit.persistEvents("event1")
    val ex =
      intercept[AssertionError] {
        raftEventJournalTestKit.receivePersisted[String](EventStore.persistenceId(), 3)
      }
    ex.getMessage should be("assertion failed: Could read only 1 events instead of expected 3")
  }

  it should "raise AssertionError when the persisted events don't correspond to expected type on receivePersisted" in {
    raftEventJournalTestKit.persistEvents(1, "event2", 3)
    val ex =
      intercept[AssertionError] {
        raftEventJournalTestKit.receivePersisted[String](EventStore.persistenceId(), 3)
      }
    ex.getMessage should be("assertion failed: Persisted events [1, 3] do not correspond to expected type")
  }

  it should "return persisted event from the first on receivePersisted after resetting" in {
    raftEventJournalTestKit.persistEvents("event1", "event2", "event3")
    val result1 =
      raftEventJournalTestKit.receivePersisted[String](EventStore.persistenceId(), 2)
    result1 should be(Seq("event1", "event2"))

    raftEventJournalTestKit.reset()

    val result2 =
      raftEventJournalTestKit.receivePersisted[String](EventStore.persistenceId(), 2)
    result2 should be(Seq("event1", "event2"))
  }

  it should "pass when any persisted events don't exist on expectNothingPersisted" in {
    // nothing to persist
    raftEventJournalTestKit.expectNothingPersisted(EventStore.persistenceId())
  }

  it should "raise AssertionError when any persisted events exist on expectNothingPersisted" in {
    raftEventJournalTestKit.persistEvents("event1", "event2")
    val ex =
      intercept[AssertionError] {
        raftEventJournalTestKit.expectNothingPersisted(EventStore.persistenceId())
      }
    ex.getMessage should be("assertion failed: Found persisted event [event1, event2], but expected nothing instead")
  }

  it should "return probe to verify tagged events" in {
    import RaftEventJournalTestKitSpec._
    val persistedEvent = TaggedEvent()
    raftEventJournalTestKit.persistEvents(persistedEvent)

    val probe = raftEventJournalTestKit.verifyEventsByTag(TaggedEvent.tag)
    probe.request(1)
    probe.expectNext().event should be(persistedEvent)
  }
}
