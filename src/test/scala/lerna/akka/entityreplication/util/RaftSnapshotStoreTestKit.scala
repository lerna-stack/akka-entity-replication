package lerna.akka.entityreplication.util

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntitySnapshot
import lerna.akka.entityreplication.raft.snapshot.{ ShardSnapshotStore, SnapshotProtocol }

import java.util.UUID

/**
  * A TestKit for testing entity snapshot operations.
  */
object RaftSnapshotStoreTestKit {

  def apply(
      system: ActorSystem,
      typeName: TypeName,
      memberIndex: MemberIndex,
      settings: ClusterReplicationSettings,
  ): RaftSnapshotStoreTestKit = new RaftSnapshotStoreTestKit(system, typeName, memberIndex, settings)
}

final class RaftSnapshotStoreTestKit(
    system: ActorSystem,
    typeName: TypeName,
    memberIndex: MemberIndex,
    settings: ClusterReplicationSettings,
) extends TestKit(system)
    with ImplicitSender {

  private var snapshotStore: ActorRef = spawnSnapshotStore()

  private def spawnSnapshotStore(): ActorRef =
    system.actorOf(
      ShardSnapshotStore.props(typeName, settings.raftSettings, memberIndex),
      s"RaftSnapshotStoreTestKitShardSnapshotStore:${UUID.randomUUID().toString}",
    )

  def snapshotStoreActorRef: ActorRef = snapshotStore

  /**
    * Saves the snapshots.
    * This operation blocks the calling thread until saving is completed.
    */
  def saveSnapshots(snapshots: Set[EntitySnapshot]): Unit = {
    snapshots.foreach { snapshot =>
      snapshotStore ! SnapshotProtocol.SaveSnapshot(snapshot, testActor)
    }
    receiveWhile(messages = snapshots.size) {
      case _: SnapshotProtocol.SaveSnapshotSuccess => Done
    }
  }

  /**
    * Fetches the snapshots of specified entities.
    */
  def fetchSnapshots(entityIds: Set[NormalizedEntityId]): Set[EntitySnapshot] = {
    entityIds.foreach { entityId =>
      snapshotStore ! SnapshotProtocol.FetchSnapshot(entityId, testActor)
    }
    receiveWhile(messages = entityIds.size) {
      case resp: SnapshotProtocol.FetchSnapshotResponse => resp
    }.collect {
      case res: SnapshotProtocol.SnapshotFound => res.snapshot
    }.toSet
  }

  /**
    * Resets [[ShardSnapshotStore]] states on memory.
    * This operation blocks the calling thread until resetting is completed.
    */
  def reset(): Unit = {
    watch(snapshotStore)
    system.stop(snapshotStore)
    expectTerminated(snapshotStore)
    snapshotStore = spawnSnapshotStore()
  }
}
