package lerna.akka.entityreplication.raft.snapshot.sync

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Status }
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension }
import akka.persistence.query.{ Offset, Sequence }
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.ActorSpec
import lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, Term }
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
  ): ActorRef =
    system.actorOf(
      SnapshotSyncManager.props(
        typeName,
        srcMemberIndex,
        dstMemberIndex,
        dstSnapshotStore,
        shardId,
        settings.raftSettings,
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
        forAll(receiveSnapshotSyncManagerPersisted[SnapshotSyncManager.SnapshotCopied](1)) { event =>
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
        SnapshotCopied(nextOffset(), srcMemberIndex, shardId, srcSnapshotTerm, srcSnapshotLogIndex1, entityIds2),
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
