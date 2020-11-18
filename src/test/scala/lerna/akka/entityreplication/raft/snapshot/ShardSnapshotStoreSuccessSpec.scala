package lerna.akka.entityreplication.raft.snapshot

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }

object ShardSnapshotStoreSuccessSpec {
  final case object DummyState
}

class ShardSnapshotStoreSuccessSpec extends TestKit(ActorSystem()) with ActorSpec {
  import ShardSnapshotStoreSuccessSpec._
  import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

  private[this] val dummyEntityState = EntityState(DummyState)

  "ShardSnapshotStore（正常系）" should {

    "SaveSnapshot に成功した場合は SaveSnapshotSuccess が返信される" in {
      val entityId      = generateUniqueEntityId()
      val snapshotStore = createShardSnapshotStore()
      val metadata      = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot      = EntitySnapshot(metadata, dummyEntityState)

      snapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotSuccess(metadata))
    }

    "FetchSnapshot に成功した場合は一度停止しても SnapshotFound でスナップショットが返信される" in {
      val entityId      = generateUniqueEntityId()
      val snapshotStore = createShardSnapshotStore()
      val metadata      = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot      = EntitySnapshot(metadata, dummyEntityState)

      snapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotSuccess(metadata))

      // terminate SnapshotStore
      snapshotStore ! PoisonPill
      watch(snapshotStore)
      expectTerminated(snapshotStore)

      val newSnapshotStore = createShardSnapshotStore()

      newSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectMsg(SnapshotFound(snapshot))
    }

    "スナップショットが無い場合は FetchSnapshot で SnapshotNotFound が返信される" in {
      val entityId      = generateUniqueEntityId()
      val snapshotStore = createShardSnapshotStore()

      // SaveSnapshot してないので、スナップショットが無い状態
      snapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectMsg(SnapshotNotFound(entityId))
    }
  }

  def createShardSnapshotStore(): ActorRef =
    planAutoKill {
      childActorOf(
        ShardSnapshotStore.props(
          "test",
          RaftSettings(system.settings.config),
          MemberIndex("test-role"),
        ),
      )
    }

  val entityIdSeq = new AtomicInteger(0)

  def generateUniqueEntityId(): NormalizedEntityId =
    NormalizedEntityId.from(s"test-entity-${entityIdSeq.incrementAndGet()}")
}
