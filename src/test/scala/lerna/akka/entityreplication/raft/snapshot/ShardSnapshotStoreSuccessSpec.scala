package lerna.akka.entityreplication.raft.snapshot

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
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

class ShardSnapshotStoreSuccessSpec extends TestKit(ActorSystem()) with ActorSpec {
  import ShardSnapshotStoreSuccessSpec._
  import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

  private[this] val dummyEntityState = EntityState(DummyState)

  "ShardSnapshotStore（正常系）" should {

    "SaveSnapshot に成功した場合は SaveSnapshotSuccess が返信される" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot           = EntitySnapshot(metadata, dummyEntityState)

      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotSuccess(metadata))
    }

    "FetchSnapshot に成功した場合は一度停止しても SnapshotFound でスナップショットが返信される" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot           = EntitySnapshot(metadata, dummyEntityState)

      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
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
  }

  def createShardSnapshotStore(additionalConfig: Config = ConfigFactory.empty()): ActorRef =
    planAutoKill {
      childActorOf(
        ShardSnapshotStore.props(
          TypeName.from("test"),
          RaftSettings(additionalConfig.withFallback(system.settings.config)),
          MemberIndex("test-role"),
        ),
      )
    }

  val entityIdSeq = new AtomicInteger(0)

  def generateUniqueEntityId(): NormalizedEntityId =
    NormalizedEntityId.from(s"test-entity-${entityIdSeq.incrementAndGet()}")
}
