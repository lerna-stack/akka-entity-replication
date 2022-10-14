package lerna.akka.entityreplication.raft.snapshot

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ ActorRef, ActorSystem }
import akka.persistence.testkit.scaladsl.SnapshotTestKit
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.ShardSnapshotStoreFailureSpec._
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }
import lerna.akka.entityreplication.testkit.KryoSerializable

object ShardSnapshotStoreFailureSpec {
  final case object DummyState extends KryoSerializable

}

class ShardSnapshotStoreFailureSpec
    extends TestKit(
      ActorSystem("ShardSnapshotStoreFailureSpec", ShardSnapshotStoreSpecBase.configWithPersistenceTestKits),
    )
    with ActorSpec {

  private val snapshotTestKit = SnapshotTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
  }

  def createShardSnapshotStore(): ActorRef =
    planAutoKill {
      childActorOf(
        ShardSnapshotStore.props(
          TypeName.from("test"),
          RaftSettings(system.settings.config),
          MemberIndex("test-role"),
        ),
      )
    }

  val entityIdSeq = new AtomicInteger(0)

  def generateUniqueEntityId(): NormalizedEntityId =
    NormalizedEntityId.from(s"test-entity-${entityIdSeq.incrementAndGet()}")

  "ShardSnapshotStore（読み込みの異常）" should {

    "FetchSnapshot に失敗した場合は応答無し（クライアント側でタイムアウトの実装が必要）" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()

      snapshotTestKit.failNextRead()
      shardSnapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectNoMessage()
    }
  }

  "ShardSnapshotStore（書き込みの異常）" should {

    "SaveSnapshot に失敗した場合は SaveSnapshotFailure が返信される" in {
      val entityId           = generateUniqueEntityId()
      val shardSnapshotStore = createShardSnapshotStore()
      val metadata           = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val dummyEntityState   = EntityState(DummyState)
      val snapshot           = EntitySnapshot(metadata, dummyEntityState)

      snapshotTestKit.failNextPersisted()
      shardSnapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotFailure(metadata))
    }
  }

}
