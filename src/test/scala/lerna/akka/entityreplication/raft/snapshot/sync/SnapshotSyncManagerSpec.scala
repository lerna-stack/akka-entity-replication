package lerna.akka.entityreplication.raft.snapshot.sync

import akka.Done
import akka.actor.Status
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.persistence.{ PersistentActor, RuntimePluginConfig }
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, Term }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.{
  EntitySnapshot,
  EntitySnapshotMetadata,
  EntityState,
}
import lerna.akka.entityreplication.raft.snapshot.{ ShardSnapshotStore, SnapshotProtocol }
import org.scalatest.Inspectors._
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpecLike }

import java.util.concurrent.atomic.AtomicInteger

object SnapshotSyncManagerSpec {

  object EventStore {
    def props(settings: ClusterReplicationSettings): Props = Props(new EventStore(settings))
    final case class PersistEvents(events: Seq[Any])
  }

  class EventStore(settings: ClusterReplicationSettings) extends PersistentActor with RuntimePluginConfig {
    import EventStore._

    override def journalPluginId: String = settings.raftSettings.journalPluginId

    override def journalPluginConfig: Config = settings.raftSettings.journalPluginAdditionalConfig

    override def snapshotPluginId: String = settings.raftSettings.snapshotStorePluginId

    override def snapshotPluginConfig: Config = ConfigFactory.empty()

    override def persistenceId: String = getClass.getCanonicalName

    override def receiveRecover: Receive = Actor.emptyBehavior

    private[this] var persisting: Int = 0

    override def receiveCommand: Receive = {
      case cmd: PersistEvents =>
        persisting = cmd.events.size
        persistAll(cmd.events.toVector) { _ =>
          persisting -= 1
          if (persisting == 0) {
            sender() ! Done
          }
        }
    }
  }
}

class SnapshotSyncManagerSpec
    extends TestKit(ActorSystem())
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach {
  import SnapshotSyncManagerSpec._

  private[this] val settings = ClusterReplicationSettings(system)

  private[this] val shardId = NormalizedShardId("test-shard")

  private[this] val srcTypeName    = TypeName.from("test-type-1")
  private[this] val srcMemberIndex = MemberIndex("test-member-index-1")
  private[this] val srcSnapshotStore =
    system.actorOf(
      ShardSnapshotStore.props(srcTypeName.underlying, settings.raftSettings, srcMemberIndex),
      "srcSnapshotStore",
    )

  private[this] val dstTypeName    = TypeName.from("test-type-2")
  private[this] val dstMemberIndex = MemberIndex("test-member-index-2")
  private[this] val dstSnapshotStore =
    system.actorOf(
      ShardSnapshotStore.props(dstTypeName.underlying, settings.raftSettings, dstMemberIndex),
      "dstSnapshotStore",
    )

  private[this] val eventStore = system.actorOf(EventStore.props(settings), "eventStore")

  private[this] val snapshotSyncManagerUniqueId = new AtomicInteger(0)

  private[this] def createSnapshotSyncManager(dstSnapshotStore: ActorRef = dstSnapshotStore): ActorRef =
    system.actorOf(
      SnapshotSyncManager.props(
        srcTypeName,
        srcMemberIndex,
        dstTypeName,
        dstMemberIndex,
        dstSnapshotStore,
        shardId,
        settings.raftSettings,
      ),
      s"snapshotSyncManager:${snapshotSyncManagerUniqueId.getAndIncrement()}",
    )

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // clear storage
    val storage = StorageExtension(system)
    storage.journalStorage ! InMemoryJournalStorage.ClearJournal
    storage.snapshotStorage ! InMemorySnapshotStorage.ClearSnapshots
    receiveWhile(messages = 2) {
      case _: Status.Success => Done
    }
  }

  "SnapshotSyncManager" should {

    "synchronize snapshots in dst member with src member by copying snapshots in src member" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("2"), srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("3"), srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(1), entityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, entityIds),
      )

      /* check */
      awaitAssert { // Persistent events may not be retrieved immediately
        createSnapshotSyncManager() ! SnapshotSyncManager.SyncSnapshot(
          srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
          srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
          dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
          dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
          replyTo = testActor,
        )
        expectMsg(SnapshotSyncManager.SyncSnapshotCompleted(srcSnapshotTerm, srcSnapshotLogIndex, srcMemberIndex))
        fetchSnapshots(entityIds, dstSnapshotStore) should be(srcSnapshots)
      }
    }

    "not update snapshots of old LogIndex which is already applied by dst member" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(2)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm      = Term(1)
      val srcSnapshotLogIndex1 = LogEntryIndex(1)
      val srcSnapshotLogIndex2 = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), srcSnapshotLogIndex1), EntityState("ignored")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("2"), srcSnapshotLogIndex2), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("3"), srcSnapshotLogIndex2), EntityState("state-3-3")),
      )
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      val entityIds1 = srcSnapshots.filter(_.metadata.logEntryIndex == srcSnapshotLogIndex1).map(_.metadata.entityId)
      val entityIds2 = srcSnapshots.filter(_.metadata.logEntryIndex == srcSnapshotLogIndex2).map(_.metadata.entityId)
      val entityIds  = entityIds1 ++ entityIds2
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex1, entityIds1),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex2, entityIds2),
      )

      srcSnapshotLogIndex1 should be <= dstSnapshotLogIndex

      /* check */
      awaitAssert { // Persistent events may not be retrieved immediately
        createSnapshotSyncManager() ! SnapshotSyncManager.SyncSnapshot(
          srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
          srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex2,
          dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
          dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
          replyTo = testActor,
        )
        expectMsg(SnapshotSyncManager.SyncSnapshotCompleted(srcSnapshotTerm, srcSnapshotLogIndex2, srcMemberIndex))
        forAtLeast(min = 1, fetchSnapshots(entityIds, dstSnapshotStore)) { snapshot =>
          snapshot.state.underlying should be("state-1-1")
        }
      }
    }

    "stop after snapshot synchronization is succeeded" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("2"), srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("3"), srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(1), entityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, entityIds),
      )

      /* check */
      val snapshotSyncManager = watch(createSnapshotSyncManager())
      snapshotSyncManager ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
        srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
        dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
        replyTo = testActor,
      )
      expectMsgType[SnapshotSyncManager.SyncSnapshotCompleted]
      expectTerminated(snapshotSyncManager)
    }

    "abort synchronizing if it founds newer than an expected snapshot" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("2"), srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(
          EntitySnapshotMetadata(NormalizedEntityId("3"), srcSnapshotLogIndex.next()),
          EntityState("state-3-new"),
        ),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(1), entityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, entityIds),
      )

      /* check */
      createSnapshotSyncManager() ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
        srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
        dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
        replyTo = testActor,
      )
      // returns the same LogIndex and Term as dst ones in the command
      expectMsg(SnapshotSyncManager.SyncSnapshotCompleted(dstSnapshotTerm, dstSnapshotLogIndex, srcMemberIndex))
      // SnapshotStore is in an inconsistent state which is updated partially
    }

    "abort synchronizing if it couldn't fetch a snapshot" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("2"), srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("3"), srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId) + NormalizedEntityId("4") // that lost the snapshot
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(1), entityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, entityIds),
      )

      /* check */
      createSnapshotSyncManager() ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
        srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
        dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
        replyTo = testActor,
      )
      // returns the same LogIndex and Term as dst ones in the command
      expectMsg(SnapshotSyncManager.SyncSnapshotCompleted(dstSnapshotTerm, dstSnapshotLogIndex, srcMemberIndex))
      // SnapshotStore is in an inconsistent state which is updated partially
    }

    "abort synchronizing if it failed saving snapshot" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("1"), srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("2"), srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("3"), srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(1), entityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, entityIds),
      )

      val brokenSnapshotStore = TestProbe()

      /* check */
      createSnapshotSyncManager(dstSnapshotStore = brokenSnapshotStore.ref) ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
        srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
        dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
        replyTo = testActor,
      )
      brokenSnapshotStore.receiveWhile(messages = srcSnapshots.size) {
        case command: SnapshotProtocol.SaveSnapshot =>
          brokenSnapshotStore.send(command.replyTo, SnapshotProtocol.SaveSnapshotFailure(command.snapshot.metadata))
      }
      // returns the same LogIndex and Term as dst ones in the command
      expectMsg(SnapshotSyncManager.SyncSnapshotCompleted(dstSnapshotTerm, dstSnapshotLogIndex, srcMemberIndex))
      // SnapshotStore is in an inconsistent state which is updated partially
    }

    "stop after snapshot synchronization is failed" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots        = Set.empty[EntitySnapshot]
      saveSnapshots(dstSnapshots, dstSnapshotStore)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots        = Set.empty[EntitySnapshot]
      val entityIds           = Set(NormalizedEntityId("1")) // that lost the snapshot
      saveSnapshots(srcSnapshots, srcSnapshotStore)
      persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, entityIds),
      )

      /* check */
      val snapshotSyncManager = watch(createSnapshotSyncManager())
      snapshotSyncManager ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
        srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
        dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
        replyTo = testActor,
      )
      expectMsgType[SnapshotSyncManager.SyncSnapshotCompleted]
      expectTerminated(snapshotSyncManager)
    }
  }

  private[this] def saveSnapshots(snapshots: Set[EntitySnapshot], snapshotStore: ActorRef): Unit = {
    snapshots.foreach { snapshot =>
      snapshotStore ! SnapshotProtocol.SaveSnapshot(snapshot, testActor)
    }
    receiveWhile(messages = snapshots.size) {
      case _: SnapshotProtocol.SaveSnapshotSuccess => Done
    }
  }

  private[this] def persistEvents(events: CompactionCompleted*): Unit = {
    eventStore ! EventStore.PersistEvents(events)
    expectMsg(Done)
  }

  private[this] def fetchSnapshots(entityIds: Set[NormalizedEntityId], snapshotStore: ActorRef): Set[EntitySnapshot] = {
    entityIds.foreach { entityId =>
      snapshotStore ! SnapshotProtocol.FetchSnapshot(entityId, testActor)
    }
    receiveWhile(messages = entityIds.size) {
      case resp: SnapshotProtocol.SnapshotFound => resp.snapshot
    }.toSet
  }
}
