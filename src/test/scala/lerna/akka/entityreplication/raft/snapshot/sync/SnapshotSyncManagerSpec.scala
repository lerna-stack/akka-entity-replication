package lerna.akka.entityreplication.raft.snapshot.sync

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.{ ActorRef, ActorSystem, Status }
import akka.actor.typed
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension }
import akka.persistence.query.{ Offset, Sequence }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettingsImpl }
import lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, Term }
import lerna.akka.entityreplication.raft.persistence.EntitySnapshotsUpdatedTag
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.{
  EntitySnapshot,
  EntitySnapshotMetadata,
  EntityState,
}
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager.{ SnapshotCopied, SyncCompleted }
import lerna.akka.entityreplication.util.{ RaftEventJournalTestKit, RaftSnapshotStoreTestKit }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Inspectors._

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class SnapshotSyncManagerSpec extends TestKit(ActorSystem()) with ActorSpec with BeforeAndAfterEach {

  private implicit val typedSystem: typed.ActorSystem[_] = system.toTyped

  private[this] val settings = ClusterReplicationSettings.create(system)

  private[this] val shardId = NormalizedShardId("test-shard")

  private[this] val typeName              = TypeName.from("test-type-1")
  private[this] val srcMemberIndex        = MemberIndex("test-member-index-1")
  private val srcRaftSnapshotStoreTestKit = RaftSnapshotStoreTestKit(system, typeName, srcMemberIndex, settings)

  private[this] val dstMemberIndex        = MemberIndex("test-member-index-2")
  private val dstRaftSnapshotStoreTestKit = RaftSnapshotStoreTestKit(system, typeName, dstMemberIndex, settings)

  private val raftEventJournalTestKit = RaftEventJournalTestKit(system, settings)

  private val snapshotSyncManagerPersistenceId: String =
    SnapshotSyncManager.persistenceId(
      typeName,
      srcMemberIndex = srcMemberIndex,
      dstMemberIndex = dstMemberIndex,
      shardId,
    )

  private def receiveSnapshotSyncManagerPersisted[A](n: Int)(implicit tag: ClassTag[A]): Seq[A] = {
    raftEventJournalTestKit.receivePersisted(snapshotSyncManagerPersistenceId, n)
  }

  private def expectSnapshotSyncManagerNothingPersisted(): Unit = {
    raftEventJournalTestKit.expectNothingPersisted(snapshotSyncManagerPersistenceId)
  }

  private[this] val snapshotSyncManagerUniqueId = new AtomicInteger(0)

  private[this] def createSnapshotSyncManager(
      dstSnapshotStore: ActorRef = dstRaftSnapshotStoreTestKit.snapshotStoreActorRef,
      overwriteConfig: Config = ConfigFactory.empty(),
  ): ActorRef =
    system.actorOf(
      SnapshotSyncManager.props(
        typeName,
        srcMemberIndex,
        dstMemberIndex,
        dstSnapshotStore,
        shardId,
        RaftSettingsImpl(overwriteConfig.withFallback(system.settings.config)),
      ),
      s"snapshotSyncManager:${snapshotSyncManagerUniqueId.getAndIncrement()}",
    )

  override def beforeEach(): Unit = {
    super.beforeEach()
    // clear storage
    val storage = StorageExtension(system)
    storage.journalStorage ! InMemoryJournalStorage.ClearJournal
    storage.snapshotStorage ! InMemorySnapshotStorage.ClearSnapshots
    receiveWhile(messages = 2) {
      case _: Status.Success => Done
    }
    // reset SnapshotStore
    srcRaftSnapshotStoreTestKit.reset()
    dstRaftSnapshotStoreTestKit.reset()
    raftEventJournalTestKit.reset()
  }

  "SnapshotSyncManager" should {

    "synchronize snapshots in dst member with src member by copying snapshots in src member" in {
      /* prepare */
      val entity1      = createUniqueEntityId()
      val entity2      = createUniqueEntityId()
      val entity3      = createUniqueEntityId()
      val allEntityIds = Set(entity1, entity2, entity3)

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm      = Term(1)
      val srcSnapshotLogIndex1 = LogEntryIndex(2)
      val srcSnapshotLogIndex2 = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex1), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex2), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex2), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)

      raftEventJournalTestKit.persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(1), entityIds),
        SnapshotCopied(nextOffset(), srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex1, Set(entity1)),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex2, Set(entity2, entity3)),
      )

      val dstEventsByTagProbe =
        raftEventJournalTestKit.probeOfEventsByTag(EntitySnapshotsUpdatedTag(dstMemberIndex, shardId).toString)
      dstEventsByTagProbe.request(1).expectNoMessage()

      /* check */
      awaitAssert { // Persistent events may not be retrieved immediately
        createSnapshotSyncManager() ! SnapshotSyncManager.SyncSnapshot(
          srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
          srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex2,
          dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
          dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
          replyTo = testActor,
        )
        expectMsg(SnapshotSyncManager.SyncSnapshotSucceeded(srcSnapshotTerm, srcSnapshotLogIndex2, srcMemberIndex))
        val persistedSnapshotCopied = receiveSnapshotSyncManagerPersisted[SnapshotSyncManager.SnapshotCopied](1)
        forAll(persistedSnapshotCopied) { event =>
          event.offset shouldNot be(null) // offset is storage-specific and non-deterministic
          event.memberIndex should be(dstMemberIndex)
          event.shardId should be(shardId)
          event.snapshotLastLogTerm should be(srcSnapshotTerm)
          event.snapshotLastLogIndex should be(srcSnapshotLogIndex2)
          event.entityIds should be(allEntityIds)
        }
        forAll(receiveSnapshotSyncManagerPersisted[SnapshotSyncManager.SyncCompleted](1)) { event =>
          event.offset shouldNot be(null) // offset is storage-specific and non-deterministic
        }
        expectSnapshotSyncManagerNothingPersisted()
        dstEventsByTagProbe.request(2) // to detect extra events
        dstEventsByTagProbe.expectNextN(1).map(_.event) should be(persistedSnapshotCopied)
        dstEventsByTagProbe.expectNoMessage()

        dstRaftSnapshotStoreTestKit.fetchSnapshots(allEntityIds) should be(srcSnapshots)
      }
    }

    "not update snapshots of old LogIndex which is already applied by dst member" in {
      /* prepare */
      val entity1 = createUniqueEntityId()
      val entity2 = createUniqueEntityId()
      val entity3 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(2)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, dstSnapshotLogIndex), EntityState("state-1-2")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm      = Term(1)
      val srcSnapshotLogIndex1 = LogEntryIndex(1)
      val srcSnapshotLogIndex2 = LogEntryIndex(2)
      val srcSnapshotLogIndex3 = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex1), EntityState("ignored")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex2), EntityState("ignored")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex3), EntityState("state-3-3")),
      )
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      val entityIds1 = srcSnapshots.filter(_.metadata.logEntryIndex == srcSnapshotLogIndex1).map(_.metadata.entityId)
      val entityIds2 = srcSnapshots.filter(_.metadata.logEntryIndex == srcSnapshotLogIndex2).map(_.metadata.entityId)
      val entityIds3 = srcSnapshots.filter(_.metadata.logEntryIndex == srcSnapshotLogIndex3).map(_.metadata.entityId)
      val entityIds  = entityIds1 ++ entityIds2 ++ entityIds3
      raftEventJournalTestKit.persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex1, entityIds1),
        SnapshotCopied(nextOffset(), srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex2, entityIds2),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex3, entityIds3),
      )

      srcSnapshotLogIndex1 should be <= dstSnapshotLogIndex
      srcSnapshotLogIndex2 should be <= dstSnapshotLogIndex

      /* check */
      awaitAssert { // Persistent events may not be retrieved immediately
        createSnapshotSyncManager() ! SnapshotSyncManager.SyncSnapshot(
          srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
          srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex3,
          dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
          dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
          replyTo = testActor,
        )
        expectMsg(SnapshotSyncManager.SyncSnapshotSucceeded(srcSnapshotTerm, srcSnapshotLogIndex3, srcMemberIndex))
        forAll(receiveSnapshotSyncManagerPersisted[SnapshotCopied](1)) { event =>
          event.entityIds should be(entityIds3)
        }
        receiveSnapshotSyncManagerPersisted[SyncCompleted](1)
        expectSnapshotSyncManagerNothingPersisted()

        val synchronizedSnapshots = dstRaftSnapshotStoreTestKit.fetchSnapshots(entityIds)
        forExactly(1, synchronizedSnapshots) { snapshot =>
          snapshot.state.underlying should be("state-1-1")
        }
        forExactly(1, synchronizedSnapshots) { snapshot =>
          snapshot.state.underlying should be("state-1-2")
        }
      }
    }

    "respond SyncSnapshotAlreadySucceeded if the dst snapshot has already synchronized to src snapshot" in {
      /* prepare */
      val entity1 = createUniqueEntityId()
      val entity2 = createUniqueEntityId()
      val entity3 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
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
        expectMsgType[SnapshotSyncManager.Response] should be(
          SnapshotSyncManager.SyncSnapshotSucceeded(srcSnapshotTerm, srcSnapshotLogIndex, srcMemberIndex),
        )
      }
      val snapshotSyncManager = watch(createSnapshotSyncManager())
      snapshotSyncManager ! SnapshotSyncManager.SyncSnapshot(
        // dst snapshot status will be synchronized to src status
        srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
        srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        dstLatestSnapshotLastLogTerm = srcSnapshotTerm,
        dstLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
        replyTo = testActor,
      )
      expectMsgType[SnapshotSyncManager.Response] should be(
        SnapshotSyncManager.SyncSnapshotAlreadySucceeded(srcSnapshotTerm, srcSnapshotLogIndex, srcMemberIndex),
      )
      expectTerminated(snapshotSyncManager)
    }

    "persist SnapshotCopied event every copying snapshots batch" in {
      /* prepare */
      val smallBatchSizeConfig = ConfigFactory.parseString {
        """
        lerna.akka.entityreplication.raft.snapshot-sync.max-snapshot-batch-size = 2
        """
      }
      val entity1      = createUniqueEntityId()
      val entity2      = createUniqueEntityId()
      val entity3      = createUniqueEntityId()
      val entity4      = createUniqueEntityId()
      val allEntityIds = Set(entity1, entity2, entity3, entity4)

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm1     = Term(1)
      val srcSnapshotTerm2     = Term(2)
      val srcSnapshotLogIndex1 = LogEntryIndex(2)
      val srcSnapshotLogIndex2 = LogEntryIndex(3)
      val srcSnapshotLogIndex3 = LogEntryIndex(4)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex1), EntityState("state-1-4")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex3), EntityState("state-2-4")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex3), EntityState("state-3-4")),
        EntitySnapshot(EntitySnapshotMetadata(entity4, srcSnapshotLogIndex3), EntityState("state-4-4")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)

      raftEventJournalTestKit.persistEvents(
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm1, LogEntryIndex(1), entityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm1, srcSnapshotLogIndex1, Set(entity1)),
        SnapshotCopied(nextOffset(), srcMemberIndex, shardId, srcSnapshotTerm2, srcSnapshotLogIndex2, Set(entity2)),
        CompactionCompleted(
          srcMemberIndex,
          shardId,
          srcSnapshotTerm2,
          srcSnapshotLogIndex3,
          Set(entity2, entity3, entity4),
        ),
      )

      /* check */
      awaitAssert { // Persistent events may not be retrieved immediately
        val manager = createSnapshotSyncManager(overwriteConfig = smallBatchSizeConfig)
        manager ! SnapshotSyncManager.SyncSnapshot(
          srcLatestSnapshotLastLogTerm = srcSnapshotTerm2,
          srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex3,
          dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
          dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
          replyTo = testActor,
        )
        expectMsg(SnapshotSyncManager.SyncSnapshotSucceeded(srcSnapshotTerm2, srcSnapshotLogIndex3, srcMemberIndex))

        forAll(receiveSnapshotSyncManagerPersisted[SnapshotSyncManager.SnapshotCopied](1)) { event =>
          // CompactionCompleted and SnapshotCopied will be merged into single SnapshotCopied.
          // The process prefers the last term, index and offset.
          event.snapshotLastLogTerm should be(srcSnapshotTerm2)
          event.snapshotLastLogIndex should be(srcSnapshotLogIndex2)
          event.entityIds should be(Set(entity1, entity2))
        }
        forAll(receiveSnapshotSyncManagerPersisted[SnapshotSyncManager.SnapshotCopied](1)) { event =>
          event.snapshotLastLogTerm should be(srcSnapshotTerm2)
          event.snapshotLastLogIndex should be(srcSnapshotLogIndex3)
          // NOTE:
          // The last CompactionCompleted event contains more than max-snapshot-batch-size of entityId.
          // The snapshots the event indicates will be copied over max-snapshot-batch-size.
          event.entityIds should be(Set(entity2, entity3, entity4))
        }
        receiveSnapshotSyncManagerPersisted[SnapshotSyncManager.SyncCompleted](1)
        expectSnapshotSyncManagerNothingPersisted()
        dstRaftSnapshotStoreTestKit.fetchSnapshots(allEntityIds) should be(srcSnapshots)
      }
    }

    "stop after snapshot synchronization is succeeded" in {
      /* prepare */
      val entity1 = createUniqueEntityId()
      val entity2 = createUniqueEntityId()
      val entity3 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
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
      expectMsgType[SnapshotSyncManager.SyncSnapshotSucceeded]
      expectTerminated(snapshotSyncManager)
    }

    "abort synchronizing if it couldn't fetch CompactionCompleted/SnapshotCopied events" in {
      /* prepare */
      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      raftEventJournalTestKit.persistEvents(
        // no events
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
        expectMsg(SnapshotSyncManager.SyncSnapshotFailed())
      }
    }

    "abort synchronizing if it founds newer than an expected snapshot" in {
      /* prepare */
      val entity1 = createUniqueEntityId()
      val entity2 = createUniqueEntityId()
      val entity3 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(
          EntitySnapshotMetadata(entity3, srcSnapshotLogIndex.next()),
          EntityState("state-3-new"),
        ),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
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
      expectMsg(SnapshotSyncManager.SyncSnapshotFailed())
      // SnapshotStore is in an inconsistent state which is updated partially
    }

    "abort synchronizing if it couldn't fetch a snapshot" in {
      /* prepare */
      val entity1 = createUniqueEntityId()
      val entity2 = createUniqueEntityId()
      val entity3 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId) + NormalizedEntityId("4") // that lost the snapshot
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
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
      expectMsg(SnapshotSyncManager.SyncSnapshotFailed())
      // SnapshotStore is in an inconsistent state which is updated partially
    }

    "abort synchronizing if it failed saving snapshot" in {
      /* prepare */
      val entity1 = createUniqueEntityId()
      val entity2 = createUniqueEntityId()
      val entity3 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-2-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity3, srcSnapshotLogIndex), EntityState("state-3-3")),
      )
      val entityIds = srcSnapshots.map(_.metadata.entityId)
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
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
      expectMsg(SnapshotSyncManager.SyncSnapshotFailed())
      // SnapshotStore is in an inconsistent state which is updated partially
    }

    "abort synchronizing if the reordered event is found (same Term, older LogEntryIndex)" in {
      /* prepare */
      val smallBatchSizeConfig = ConfigFactory.parseString {
        """
        // This settings allows us to verify that SnapshotSyncManager can detect error even if events are handled in small batch
        lerna.akka.entityreplication.raft.snapshot-sync.max-snapshot-batch-size = 1
        """
      }
      val entity1      = createUniqueEntityId()
      val entity2      = createUniqueEntityId()
      val allEntityIds = Set(entity1, entity2)

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-1-3")),
      )
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
        // CompactionCompleted has an older index than SnapshotCopied
        SnapshotCopied(nextOffset(), srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex, allEntityIds),
        CompactionCompleted(srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex.prev(), allEntityIds),
      )

      /* check */
      LoggingTestKit
        .error("It must process events in ascending order of snapshotLastLogTerm and snapshotLastLogIndex").expect {
          createSnapshotSyncManager(overwriteConfig = smallBatchSizeConfig) ! SnapshotSyncManager.SyncSnapshot(
            srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
            srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
            dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
            dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
            replyTo = testActor,
          )
          expectMsg(SnapshotSyncManager.SyncSnapshotFailed())
        }
    }

    "abort synchronizing if the reordered event is found (older Term, newer LogEntryIndex) (usually impossible)" in {
      /* prepare */
      val smallBatchSizeConfig = ConfigFactory.parseString {
        """
        // This settings allows us to verify that SnapshotSyncManager can detect error even if events are handled in small batch
        lerna.akka.entityreplication.raft.snapshot-sync.max-snapshot-batch-size = 1
        """
      }
      val entity1      = createUniqueEntityId()
      val entity2      = createUniqueEntityId()
      val allEntityIds = Set(entity1, entity2)

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, dstSnapshotLogIndex), EntityState("state-1-1")),
      )
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(2)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entity1, srcSnapshotLogIndex), EntityState("state-1-3")),
        EntitySnapshot(EntitySnapshotMetadata(entity2, srcSnapshotLogIndex), EntityState("state-2-3")),
      )
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
        // CompactionCompleted has an older Term than SnapshotCopied but has an newer LogEntryIndex (usually impossible)
        SnapshotCopied(nextOffset(), srcMemberIndex, shardId, srcSnapshotTerm, LogEntryIndex(2), allEntityIds),
        CompactionCompleted(srcMemberIndex, shardId, Term(1), srcSnapshotLogIndex, allEntityIds),
      )

      /* check */
      LoggingTestKit
        .error("It must process events in ascending order of snapshotLastLogTerm and snapshotLastLogIndex").expect {
          createSnapshotSyncManager(overwriteConfig = smallBatchSizeConfig) ! SnapshotSyncManager.SyncSnapshot(
            srcLatestSnapshotLastLogTerm = srcSnapshotTerm,
            srcLatestSnapshotLastLogIndex = srcSnapshotLogIndex,
            dstLatestSnapshotLastLogTerm = dstSnapshotTerm,
            dstLatestSnapshotLastLogIndex = dstSnapshotLogIndex,
            replyTo = testActor,
          )
          expectMsg(SnapshotSyncManager.SyncSnapshotFailed())
        }
    }

    "stop after snapshot synchronization is failed" in {
      /* prepare */
      val entity1 = createUniqueEntityId()

      val dstSnapshotTerm     = Term(1)
      val dstSnapshotLogIndex = LogEntryIndex(1)
      val dstSnapshots        = Set.empty[EntitySnapshot]
      dstRaftSnapshotStoreTestKit.saveSnapshots(dstSnapshots)

      val srcSnapshotTerm     = Term(1)
      val srcSnapshotLogIndex = LogEntryIndex(3)
      val srcSnapshots        = Set.empty[EntitySnapshot]
      val entityIds           = Set(entity1) // that lost the snapshot
      srcRaftSnapshotStoreTestKit.saveSnapshots(srcSnapshots)
      raftEventJournalTestKit.persistEvents(
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
      expectMsgType[SnapshotSyncManager.SyncSnapshotFailed]
      expectTerminated(snapshotSyncManager)
    }
  }

  lazy val nextOffset: () => Offset = {
    var offset = 0L
    def next() = {
      offset += 1
      offset
    }
    () => Sequence(next())
  }

  lazy val createUniqueEntityId: () => NormalizedEntityId = {
    var entityId = 0
    def next() = {
      entityId += 1
      entityId
    }
    () => NormalizedEntityId.from(s"Entity-${next()}")
  }
}
