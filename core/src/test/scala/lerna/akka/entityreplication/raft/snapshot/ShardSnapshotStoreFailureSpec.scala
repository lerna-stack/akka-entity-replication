package lerna.akka.entityreplication.raft.snapshot

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.persistence.testkit._
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }
import lerna.akka.entityreplication.testkit.KryoSerializable

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise

object ShardSnapshotStoreFailureSpec {
  final case object DummyState extends KryoSerializable
}

class ShardSnapshotStoreFailureSpec
    extends TestKit(
      ActorSystem("ShardSnapshotStoreFailureSpec", ShardSnapshotStoreSpecBase.configWithPersistenceTestKits),
    )
    with ActorSpec {
  import ShardSnapshotStoreFailureSpec._

  private val persistenceTestKit = PersistenceTestKit(system)
  private val snapshotTestKit    = SnapshotTestKit(system)
  private val typeName           = TypeName.from("test")
  private val memberIndex        = MemberIndex("test-role")

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
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

  "ShardSnapshotStore（読み込みの異常）" should {

    "FetchSnapshot に失敗した場合は応答無し（クライアント側でタイムアウトの実装が必要）" in {
      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore()
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)

      snapshotTestKit.failNextRead(snapshotStorePersistenceId)
      shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectNoMessage()
    }
  }

  "ShardSnapshotStore（書き込みの異常）" should {

    "SaveSnapshot に失敗した場合は SaveSnapshotFailure が返信される" in {
      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore()
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)
      val metadata                   = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val dummyEntityState           = EntityState(DummyState)
      val snapshot                   = EntitySnapshot(metadata, dummyEntityState)

      persistenceTestKit.failNextPersisted(snapshotStorePersistenceId)
      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotFailure(metadata))
    }

    "output warn log when save EntitySnapshot as a snapshot failed" in {
      implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

      val config = ConfigFactory
        .parseString("""
                       |lerna.akka.entityreplication.raft.entity-snapshot-store.snapshot-every = 1
                       |""".stripMargin)
        .withFallback(system.settings.config)
      val entityId                   = generateUniqueEntityId()
      val shardSnapshotStore         = createShardSnapshotStore(additionalConfig = config)
      val snapshotStorePersistenceId = SnapshotStore.persistenceId(typeName, entityId, memberIndex)
      val metadata                   = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot                   = EntitySnapshot(metadata, EntityState(DummyState))

      // Prepare:
      snapshotTestKit.failNextPersisted(snapshotStorePersistenceId)

      // Test:
      LoggingTestKit.warn("Failed to saveSnapshot").expect {
        shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
        persistenceTestKit.expectNextPersisted(snapshotStorePersistenceId, snapshot)
        expectMsg(SaveSnapshotSuccess(metadata))

        snapshotTestKit.expectNothingPersisted(snapshotStorePersistenceId)
      }
    }
  }

  "ShardSnapshotStore (with time-consuming writes)" should {

    // Emulates a time-consuming write
    // Note:
    //   The promise (`processingResultPromise`) must be fulfilled.
    //   The succeeding tests will fail unless the promise is fulfilled.
    class TimeConsumingPersistEventPolicy extends EventStorage.JournalPolicies.PolicyType {
      private val processingResultPromise = Promise[ProcessingResult]()
      override def tryProcess(persistenceId: String, processingUnit: JournalOperation): ProcessingResult = {
        processingUnit match {
          case _: WriteEvents => processingResultPromise.future.await
          case _              => ProcessingSuccess
        }
      }
      def trySuccess(): Unit = {
        processingResultPromise.trySuccess(ProcessingSuccess)
      }
    }

    "reply with `SnapshotNotFound` to `FetchSnapshot` if it has no EntitySnapshot and is saving an EntitySnapshot" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex(1))
      val snapshot           = EntitySnapshot(metadata, EntityState(DummyState))

      val timeConsumingPersistEventPolicy = new TimeConsumingPersistEventPolicy()
      try {
        // Prepare: SnapshotStore is saving the snapshot
        persistenceTestKit.withPolicy(timeConsumingPersistEventPolicy)
        shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)

        // Test:
        shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
        expectMsg(SnapshotNotFound(entityId))
      } finally {
        // Cleanup:
        // The succeeding tests will fail unless the promise is fulfilled.
        timeConsumingPersistEventPolicy.trySuccess()
      }
    }

    "reply with `SnapshotFound` to `FetchSnapshot` if it has an EntitySnapshot and is saving a new EntitySnapshot" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()

      val firstSnapshotMetadata = EntitySnapshotMetadata(entityId, LogEntryIndex(1))
      val firstSnapshot =
        EntitySnapshot(firstSnapshotMetadata, EntityState(DummyState))
      shardSnapshotStore ! SaveSnapshot(firstSnapshot, replyTo = testActor)
      expectMsg(SaveSnapshotSuccess(firstSnapshotMetadata))

      val timeConsumingPersistEventPolicy = new TimeConsumingPersistEventPolicy()
      try {
        // Prepare: SnapshotStore is saving the second snapshot
        persistenceTestKit.withPolicy(timeConsumingPersistEventPolicy)
        val secondSnapshot =
          EntitySnapshot(EntitySnapshotMetadata(entityId, LogEntryIndex(5)), EntityState(DummyState))
        shardSnapshotStore ! SaveSnapshot(secondSnapshot, replyTo = testActor)

        // Test:
        shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
        expectMsg(SnapshotFound(firstSnapshot))
      } finally {
        // Cleanup:
        // The succeeding tests will fail unless the promise is fulfilled.
        timeConsumingPersistEventPolicy.trySuccess()
      }
    }

    "reply with nothing to `SaveSnapshot` and log a warning if it is saving an EntitySnapshot" in {
      implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex(1))
      val snapshot           = EntitySnapshot(metadata, EntityState(DummyState))

      val timeConsumingPersistEventPolicy = new TimeConsumingPersistEventPolicy()
      try {
        // Prepare: SnapshotStore is saving the snapshot
        persistenceTestKit.withPolicy(timeConsumingPersistEventPolicy)
        shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)

        // Test:
        LoggingTestKit
          .warn(
            s"Saving snapshot for an entity ($entityId) currently. Consider to increase log-size-threshold or log-size-check-interval.",
          ).expect {
            shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
          }
        expectNoMessage()
      } finally {
        // Cleanup:
        // The succeeding tests will fail unless the promise is fulfilled.
        timeConsumingPersistEventPolicy.trySuccess()
      }
    }

  }

}
