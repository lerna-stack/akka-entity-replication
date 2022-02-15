package lerna.akka.entityreplication.util

import akka.actor.ActorSystem
import akka.actor.Status
import akka.testkit.{ ImplicitSender, TestKit }
import lerna.akka.entityreplication.typed.ClusterReplicationSettings
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, StorageExtension }
import org.scalatest.{ BeforeAndAfterEach, FlatSpecLike, Matchers }

class RaftEventJournalTestKitSpec
    extends TestKit(ActorSystem("RaftSnapshotStoreTestKitSpec"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterEach
    with ImplicitSender {

  private val settings = ClusterReplicationSettings(system.toTyped)

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
}
