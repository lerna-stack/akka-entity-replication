package lerna.akka.entityreplication.raft.snapshot

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }
import lerna.akka.entityreplication.testkit.KryoSerializable

object ShardSnapshotStoreSuccessSpec {
  final case object DummyState extends KryoSerializable
}

class ShardSnapshotStoreSuccessSpec
    extends TestKit(
      ActorSystem("ShardSnapshotStoreSuccessSpec", ShardSnapshotStoreSpecBase.configWithPersistenceTestKits),
    )
    with ActorSpec {
  import ShardSnapshotStoreSuccessSpec._
  import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

  private val persistenceTestKit     = PersistenceTestKit(system)
  private val snapshotTestKit        = SnapshotTestKit(system)
  private val typeName               = TypeName.from("test")
  private val memberIndex            = MemberIndex("test-role")
  private[this] val dummyEntityState = EntityState(DummyState)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
  }

  "ShardSnapshotStore（正常系）" should {

    "SaveSnapshot に成功した場合は SaveSnapshotSuccess が返信される" in {
      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore()
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)
      val metadata                   = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot                   = EntitySnapshot(metadata, dummyEntityState)

      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      persistenceTestKit.expectNextPersisted(snapshotStorePersistenceId, snapshot)
      expectMsg(SaveSnapshotSuccess(metadata))
    }

    "persist nothing and reply with SaveSnapshotSuccess to SaveSnapshot if it has the same EntitySnapshot" in {
      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore()
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)
      val metadata                   = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot                   = EntitySnapshot(metadata, dummyEntityState)

      // Prepare:
      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      persistenceTestKit.expectNextPersisted(snapshotStorePersistenceId, snapshot)
      expectMsg(SaveSnapshotSuccess(metadata))

      // Test:
      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      persistenceTestKit.expectNothingPersisted(snapshotStorePersistenceId)
      expectMsg(SaveSnapshotSuccess(metadata))
    }

    "FetchSnapshot に成功した場合は一度停止しても SnapshotFound でスナップショットが返信される" in {
      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore()
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)
      val metadata                   = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot                   = EntitySnapshot(metadata, dummyEntityState)

      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      persistenceTestKit.expectNextPersisted(snapshotStorePersistenceId, snapshot)
      expectMsg(SaveSnapshotSuccess(metadata))

      // terminate SnapshotStore
      shardSnapshotStore ! PoisonPill
      watch(shardSnapshotStore)
      expectTerminated(shardSnapshotStore)

      val newShardSnapshotStore = createShardSnapshotStore()

      newShardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectMsg(SnapshotFound(snapshot))
    }

    "スナップショットが無い場合は FetchSnapshot で SnapshotNotFound が返信される" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()

      // SaveSnapshot してないので、スナップショットが無い状態
      shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectMsg(SnapshotNotFound(entityId))
    }

    "処理が完了したら SnapshotStore Actor が停止する" in {
      val additionalConfig = ConfigFactory.parseString("""
                                                         |lerna.akka.entityreplication.raft.compaction.snapshot-cache-time-to-live = 1s // < test timeout 3s
                                                         |""".stripMargin)

      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore(additionalConfig)
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot           = EntitySnapshot(metadata, dummyEntityState)

      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsgType[SaveSnapshotSuccess]

      val snapshotStore = lastSender
      watch(snapshotStore)
      expectTerminated(snapshotStore)
    }

    "save EntitySnapshot as a snapshot per lerna.akka.entityreplication.raft.snapshot-store.snapshot-every" in {
      implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore()
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)
      val metadata                   = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot                   = EntitySnapshot(metadata, dummyEntityState)

      LoggingTestKit.debug("Saving EntitySnapshot as a snapshot succeeded.").expect {
        // In this test, lerna.akka.entityreplication.raft.snapshot-store.snapshot-every = 1
        shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
        persistenceTestKit.expectNextPersisted(snapshotStorePersistenceId, snapshot)
        expectMsg(SaveSnapshotSuccess(metadata))

        snapshotTestKit.expectNextPersisted(snapshotStorePersistenceId, snapshot)
      }
    }
  }

  def createShardSnapshotStore(additionalConfig: Config = ConfigFactory.empty()): ActorRef =
    planAutoKill {
      childActorOf(
        ShardSnapshotStore.props(
          typeName,
          RaftSettings(additionalConfig.withFallback(system.settings.config)),
          memberIndex,
        ),
      )
    }

  val entityIdSeq = new AtomicInteger(0)

  def generateUniqueEntityId(): NormalizedEntityId =
    NormalizedEntityId.from(s"test-entity-${entityIdSeq.incrementAndGet()}")
}
