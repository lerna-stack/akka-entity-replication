package lerna.akka.entityreplication.util

import akka.actor.{ Actor, ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FlatSpecLike, Inside, Matchers }

import scala.concurrent.{ ExecutionException, Future }

class RaftSnapshotStoreTestKitSpec
    extends TestKit(ActorSystem("RaftSnapshotStoreTestKitSpec"))
    with FlatSpecLike
    with Matchers
    with ScalaFutures
    with Inside {

  private val shardSnapshotStoreProbe = TestProbe()

  private class ShardSnapshotStoreProbeBridge extends Actor {
    override def receive: Receive = msg => shardSnapshotStoreProbe.ref forward msg
  }

  private val raftSnapshotStoreTestKit = new RaftSnapshotStoreTestKit(system, Props(new ShardSnapshotStoreProbeBridge))

  import system.dispatcher

  behavior of "RaftSnapshotStoreTestKit"

  it should "send SaveSnapshots to SnapshotStore on saveSnapshot" in {
    val snapshots =
      Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), LogEntryIndex(1)), EntityState("a")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-2"), LogEntryIndex(1)), EntityState("b")),
      )
    val operation =
      Future {
        raftSnapshotStoreTestKit.saveSnapshots(snapshots)
      }
    val savedSnapshots =
      shardSnapshotStoreProbe.receiveWhile(messages = 2) {
        case command: SaveSnapshot =>
          command.replyTo ! SaveSnapshotSuccess(command.snapshot.metadata)
          command.snapshot
      }
    operation.transformWith(Future.successful).futureValue.isSuccess should be(true)
    savedSnapshots should contain allElementsOf snapshots
  }

  it should "raise AssertionError when SnapshotStore replies SaveSnapshotFailure on saveSnapshot" in {
    val snapshots =
      Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), LogEntryIndex(1)), EntityState("a")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-2"), LogEntryIndex(1)), EntityState("b")),
      )

    val operation =
      Future {
        raftSnapshotStoreTestKit.saveSnapshots(snapshots)
      }
    // SnapshotStore will receive 2 SaveSnapshot
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case command: SaveSnapshot =>
        command.replyTo ! SaveSnapshotSuccess(command.snapshot.metadata)
    }
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case command: SaveSnapshot =>
        command.replyTo ! SaveSnapshotFailure(command.snapshot.metadata)
    }

    inside(operation.failed.futureValue) {
      case ex: ExecutionException =>
        inside(ex.getCause) {
          case ex: AssertionError =>
            ex.getMessage should be(
              "assertion failed: Failed to save snapshots: The expected number of the saved snapshot is 2 but the actual 1",
            )
        }
    }
  }

  it should "raise AssertionError when SnapshotStore doesn't reply commands sufficiently on saveSnapshot" in {
    val snapshots =
      Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), LogEntryIndex(1)), EntityState("a")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-2"), LogEntryIndex(1)), EntityState("b")),
      )

    // We have to wait longer than default timeout
    implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = remainingOrDefault * 2)
    val operation =
      Future {
        raftSnapshotStoreTestKit.saveSnapshots(snapshots)
      }
    // SnapshotStore will receive 2 SaveSnapshot
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case command: SaveSnapshot =>
        command.replyTo ! SaveSnapshotSuccess(command.snapshot.metadata)
    }
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case _: SaveSnapshot => // don't reply
    }

    inside(operation.failed.futureValue) {
      case ex: ExecutionException =>
        inside(ex.getCause) {
          case ex: AssertionError =>
            ex.getMessage should be(
              s"assertion failed: timeout (${remainingOrDefault}) while expecting 2 messages (got 1)",
            )
        }
    }
  }

  it should "send FetchSnapshot to SnapshotStore on fetchSnapshots" in {
    val snapshots =
      Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), LogEntryIndex(1)), EntityState("a")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-2"), LogEntryIndex(1)), EntityState("b")),
      )
    val entityIds = snapshots.map(_.metadata.entityId)
    val operation =
      Future {
        raftSnapshotStoreTestKit.fetchSnapshots(entityIds)
      }
    val queriedEntityId =
      shardSnapshotStoreProbe.receiveWhile(messages = 2) {
        case command: FetchSnapshot =>
          inside(snapshots.find(_.metadata.entityId == command.entityId)) {
            case Some(snapshot) =>
              command.replyTo ! SnapshotFound(snapshot)
              command.entityId
          }
      }

    queriedEntityId should contain theSameElementsAs entityIds
    operation.futureValue should contain theSameElementsAs snapshots
  }

  it should "raise AssertionError when SnapshotStore replies SnapshotNotFound on fetchSnapshots" in {
    val snapshots =
      Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), LogEntryIndex(1)), EntityState("a")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-2"), LogEntryIndex(1)), EntityState("b")),
      )
    val entityIds = snapshots.map(_.metadata.entityId)
    val operation =
      Future {
        raftSnapshotStoreTestKit.fetchSnapshots(entityIds)
      }
    // SnapshotStore will receive 2 FetchSnapshot
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case command: FetchSnapshot =>
        inside(snapshots.find(_.metadata.entityId == command.entityId)) {
          case Some(snapshot) =>
            command.replyTo ! SnapshotFound(snapshot)
        }
    }
    val notFoundEntityId =
      inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
        case command: FetchSnapshot =>
          command.replyTo ! SnapshotNotFound(command.entityId)
          command.entityId
      }

    inside(operation.failed.futureValue) {
      case ex: ExecutionException =>
        inside(ex.getCause) {
          case ex: AssertionError =>
            ex.getMessage should be(
              s"assertion failed: Failed to fetch snapshots of [${notFoundEntityId.underlying}]",
            )
        }
    }
  }

  it should "raise AssertionError when SnapshotStore doesn't reply commands sufficiently on fetchSnapshots" in {
    val snapshots =
      Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), LogEntryIndex(1)), EntityState("a")),
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-2"), LogEntryIndex(1)), EntityState("b")),
      )
    val entityIds = snapshots.map(_.metadata.entityId)
    // We have to wait longer than default timeout
    implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = remainingOrDefault * 2)
    val operation =
      Future {
        raftSnapshotStoreTestKit.fetchSnapshots(entityIds)
      }
    // SnapshotStore will receive 2 FetchSnapshot
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case command: FetchSnapshot =>
        inside(snapshots.find(_.metadata.entityId == command.entityId)) {
          case Some(snapshot) =>
            command.replyTo ! SnapshotFound(snapshot)
        }
    }
    inside(shardSnapshotStoreProbe.expectMsgType[Command]) {
      case _: FetchSnapshot => // don't reply
    }

    inside(operation.failed.futureValue) {
      case ex: ExecutionException =>
        inside(ex.getCause) {
          case ex: AssertionError =>
            ex.getMessage should be(
              s"assertion failed: timeout (${remainingOrDefault}) while expecting 2 messages (got 1)",
            )
        }
    }
  }
}
