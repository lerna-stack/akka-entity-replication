package lerna.akka.entityreplication.raft.snapshot

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.ShardSnapshotStoreFailureSpecBase._
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }

// snapshot-store のスタブを利用して snapshot の読み込みを失敗させる
class ShardSnapshotStoreLoadingFailureSpec
    extends ShardSnapshotStoreFailureSpecBase(
      SnapshotPluginStub.brokenLoadingSnapshotConfig.withFallback(ConfigFactory.load()),
    ) {

  "ShardSnapshotStore（読み込みの異常）" should {

    "FetchSnapshot に失敗した場合は応答無し（クライアント側でタイムアウトの実装が必要）" in {
      val entityId      = generateUniqueEntityId()
      val snapshotStore = createShardSnapshotStore()

      snapshotStore ! FetchSnapshot(entityId, replyTo = testActor)
      expectNoMessage()
    }
  }
}

// snapshot-store のスタブを利用して snapshot の永続化を失敗させる
class ShardSnapshotStoreSavingFailureSpec
    extends ShardSnapshotStoreFailureSpecBase(
      SnapshotPluginStub.brokenSavingSnapshotConfig.withFallback(ConfigFactory.load()),
    ) {

  private[this] val dummyEntityState = EntityState(DummyState)

  "ShardSnapshotStore（書き込みの異常）" should {

    "SaveSnapshot に失敗した場合は SaveSnapshotFailure が返信される" in {
      val entityId      = generateUniqueEntityId()
      val snapshotStore = createShardSnapshotStore()
      val metadata      = EntitySnapshotMetadata(entityId, LogEntryIndex.initial())
      val snapshot      = EntitySnapshot(metadata, dummyEntityState)

      snapshotStore ! SaveSnapshot(snapshot, replyTo = testActor)
      expectMsg(SaveSnapshotFailure(metadata))
    }
  }
}

object ShardSnapshotStoreFailureSpecBase {
  final case object DummyState
}

abstract class ShardSnapshotStoreFailureSpecBase(config: Config)
    extends TestKit(ActorSystem("ShardSnapshotStoreFailureSpec", config))
    with ActorSpec {

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
}
