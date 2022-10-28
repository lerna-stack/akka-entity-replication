package lerna.akka.entityreplication.raft.snapshot

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.testkit.scaladsl.SnapshotTestKit
import akka.persistence.testkit.{
  ProcessingResult,
  ProcessingSuccess,
  SnapshotOperation,
  SnapshotStorage,
  WriteSnapshot,
}
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }
import lerna.akka.entityreplication.testkit.KryoSerializable

import scala.concurrent.Promise
import scala.util.Using

object ShardSnapshotStoreFailureSpec {
  final case object DummyState extends KryoSerializable
}

class ShardSnapshotStoreFailureSpec
    extends TestKit(
      ActorSystem("ShardSnapshotStoreFailureSpec", ShardSnapshotStoreSpecBase.configWithPersistenceTestKits),
    )
    with ActorSpec {
  import ShardSnapshotStoreFailureSpec._

  private val snapshotTestKit = SnapshotTestKit(system)
  private val typeName        = TypeName.from("test")
  private val memberIndex     = MemberIndex("test-role")

  override def beforeEach(): Unit = {
    super.beforeEach()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
  }

  def createShardSnapshotStore(): ActorRef =
    planAutoKill {
      childActorOf(
        ShardSnapshotStore.props(
          typeName,
          RaftSettings(system.settings.config),
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

      snapshotTestKit.failNextPersisted(snapshotStorePersistenceId)
      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotFailure(metadata))
    }
  }

  "ShardSnapshotStore (with time-consuming writes)" should {

    // Emulates a time-consuming write
    class TimeConsumingWriteSnapshotPolicy extends SnapshotStorage.SnapshotPolicies.PolicyType with AutoCloseable {
      val processingResultPromise = Promise[ProcessingResult]()
      override def tryProcess(persistenceId: String, processingUnit: SnapshotOperation): ProcessingResult = {
        processingUnit match {
          case _: WriteSnapshot => processingResultPromise.future.await
          case _                => ProcessingSuccess
        }
      }
      override def close(): Unit = {
        processingResultPromise.trySuccess(ProcessingSuccess)
      }
    }

    "reply with `SnapshotNotFound` to `FetchSnapshot` if it has no EntitySnapshot and is saving an EntitySnapshot" ignore {
      // TODO Change SnapshotStore.savingSnapshot such that this test passes.
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex(1))
      val snapshot           = EntitySnapshot(metadata, EntityState(DummyState))

      Using(new TimeConsumingWriteSnapshotPolicy()) { timeConsumingWriteSnapshotPolicy =>
        // Prepare: SnapshotStore is saving the snapshot
        snapshotTestKit.withPolicy(timeConsumingWriteSnapshotPolicy)
        shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)

        // Test:
        shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
        expectMsg(SnapshotNotFound)
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

      Using(new TimeConsumingWriteSnapshotPolicy()) { timeConsumingWriteSnapshotPolicy =>
        // Prepare: SnapshotStore is saving the second snapshot
        snapshotTestKit.withPolicy(timeConsumingWriteSnapshotPolicy)
        val secondSnapshot =
          EntitySnapshot(EntitySnapshotMetadata(entityId, LogEntryIndex(5)), EntityState(DummyState))
        shardSnapshotStore ! SaveSnapshot(secondSnapshot, replyTo = testActor)

        // Test:
        shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
        expectMsg(SnapshotFound(firstSnapshot))
      }
    }

    "reply with nothing to `SaveSnapshot` and log a warning if it is saving an EntitySnapshot" in {
      implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex(1))
      val snapshot           = EntitySnapshot(metadata, EntityState(DummyState))

      Using(new TimeConsumingWriteSnapshotPolicy()) { timeConsumingWriteSnapshotPolicy =>
        // Prepare: SnapshotStore is saving the snapshot
        snapshotTestKit.withPolicy(timeConsumingWriteSnapshotPolicy)
        shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)

        // Test:
        LoggingTestKit
          .warn(
            s"Saving snapshot for an entity ($entityId) currently. Consider to increase log-size-threshold or log-size-check-interval.",
          ).expect {
            shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
          }
        expectNoMessage()
      }
    }

  }

}
