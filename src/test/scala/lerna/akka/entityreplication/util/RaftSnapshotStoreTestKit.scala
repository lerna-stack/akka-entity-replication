package lerna.akka.entityreplication.util

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props }
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
  ): RaftSnapshotStoreTestKit =
    new RaftSnapshotStoreTestKit(system, ShardSnapshotStore.props(typeName, settings.raftSettings, memberIndex))
}

final class RaftSnapshotStoreTestKit private[util] (
    system: ActorSystem,
    shardSnapshotStoreProps: Props,
) extends TestKit(system)
    with ImplicitSender {

  private var snapshotStore: ActorRef = spawnSnapshotStore()

  private def spawnSnapshotStore(): ActorRef =
    system.actorOf(
      shardSnapshotStoreProps,
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
    val results =
      receiveN(snapshots.size).collect {
        case _: SnapshotProtocol.SaveSnapshotSuccess => Done
      }
    assert(
      results.size == snapshots.size,
      s"Failed to save snapshots: The expected number of the saved snapshot is ${snapshots.size} but the actual ${results.size}",
    )
  }

  /**
    * Fetches the snapshots of specified entities.
    */
  def fetchSnapshots(entityIds: Set[NormalizedEntityId]): Set[EntitySnapshot] = {
    entityIds.foreach { entityId =>
      snapshotStore ! SnapshotProtocol.FetchSnapshot(entityId, testActor)
    }
    val results =
      receiveN(entityIds.size).collect {
        case res: SnapshotProtocol.SnapshotFound => res.snapshot
      }.toSet
    val shortages = entityIds.diff(results.map(_.metadata.entityId))
    assert(shortages.isEmpty, s"Failed to fetch snapshots of [${shortages.map(_.underlying).mkString(", ")}]")
    results
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
