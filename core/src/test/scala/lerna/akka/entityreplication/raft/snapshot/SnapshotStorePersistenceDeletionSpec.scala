package lerna.akka.entityreplication.raft.snapshot

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ typed, ActorRef, ActorSystem }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll

import java.util.UUID

final class SnapshotStorePersistenceDeletionSpec
    extends TestKit(
      ActorSystem("SnapshotStorePersistenceDeletionSpec", ShardSnapshotStoreSpecBase.configWithPersistenceTestKits),
    )
    with ActorSpec
    with BeforeAndAfterAll
    with TypeCheckedTripleEquals {

  import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
  private val persistenceTestKit                               = PersistenceTestKit(system)
  private val snapshotTestKit                                  = SnapshotTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
  }

  override def afterAll(): Unit = {
    try shutdown(system)
    finally super.afterAll()
  }

  private def configFor(
      snapshotEvery: Int,
      deleteBeforeRelativeSequenceNr: Long,
      deleteOldEvents: Boolean = false,
      deleteOldSnapshots: Boolean = false,
  ): Config = {
    ConfigFactory
      .parseString(s"""
          |lerna.akka.entityreplication.raft.entity-snapshot-store {
          |  snapshot-every = ${snapshotEvery}
          |  delete-old-events = ${deleteOldEvents}
          |  delete-old-snapshots = ${deleteOldSnapshots}
          |  delete-before-relative-sequence-nr = ${deleteBeforeRelativeSequenceNr}
          |}
          |""".stripMargin)
      .withFallback(system.settings.config)
  }

  private trait Fixture {
    val typeName: TypeName           = TypeName.from("test")
    val memberIndex: MemberIndex     = MemberIndex("test-role")
    val entityId: NormalizedEntityId = NormalizedEntityId.from(s"entity-${UUID.randomUUID().toString}")

    val persistenceId: String =
      SnapshotStore.persistenceId(typeName, entityId, memberIndex)

    val probe: TestProbe = TestProbe()

    def spawnSnapshotStore(config: Config): ActorRef =
      planAutoKill {
        childActorOf(
          SnapshotStore.props(
            typeName,
            entityId,
            RaftSettings(config),
            memberIndex,
          ),
        )
      }

    def createEntitySnapshotData(index: LogEntryIndex): EntitySnapshot =
      EntitySnapshot(EntitySnapshotMetadata(entityId, index), EntityState(s"state-${entityId.raw}-${index}"))

    def saveEntitySnapshot(snapshotStore: ActorRef, entitySnapshot: EntitySnapshot): Unit = {
      probe.send(snapshotStore, SaveSnapshot(entitySnapshot, replyTo = probe.ref))
      probe.expectMsg(SaveSnapshotSuccess(entitySnapshot.metadata))
    }
  }

  "not delete events if event deletion is disabled" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 2,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldEvents = false,
      ),
    )

    // Arrange: save the first event.
    val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
    saveEntitySnapshot(snapshotStore, entityState1)
    persistenceTestKit.expectNextPersisted(persistenceId, entityState1)

    // Act: save the second event, which will trigger a snapshot save but not event deletion.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)
    persistenceTestKit.expectNextPersisted(persistenceId, entityState2)

    // Assert:
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)
    assertForDuration(
      {
        assert(persistenceTestKit.persistedInStorage(persistenceId) === Seq(entityState1, entityState2))
      },
      max = remainingOrDefault,
    )
  }

  "not delete events if it fails a snapshot save" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 2,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldEvents = true,
      ),
    )

    // Arrange: save the first event.
    val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
    saveEntitySnapshot(snapshotStore, entityState1)
    persistenceTestKit.expectNextPersisted(persistenceId, entityState1)

    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    LoggingTestKit.warn("Failed to saveSnapshot").expect {
      // Arrange: the next snapshot save will fail.
      snapshotTestKit.failNextPersisted(persistenceId)
      // Act: save the second event, which will trigger a snapshot save.
      saveEntitySnapshot(snapshotStore, entityState2)
      persistenceTestKit.expectNextPersisted(persistenceId, entityState2)
    }

    // Assert:
    assertForDuration(
      {
        assert(persistenceTestKit.persistedInStorage(persistenceId) === Seq(entityState1, entityState2))
      },
      max = remainingOrDefault,
    )
  }

  "delete events matching the criteria if it successfully saves a snapshot" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 2,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldEvents = true,
      ),
    )

    // Arrange: save the first event.
    locally {
      val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
      saveEntitySnapshot(snapshotStore, entityState1)
      persistenceTestKit.expectNextPersisted(persistenceId, entityState1)
    }

    // Act: save the second event, which will trigger a snapshot save and event deletion.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)

    // Assert:
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)
    awaitAssert {
      assert(persistenceTestKit.persistedInStorage(persistenceId) === Seq(entityState2))
    }
  }

  "not delete events if it successfully saves a snapshot, but no events match the criteria" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 2,
        deleteBeforeRelativeSequenceNr = 10,
        deleteOldEvents = true,
      ),
    )

    // Arrange: save the first event.
    val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
    saveEntitySnapshot(snapshotStore, entityState1)
    persistenceTestKit.expectNextPersisted(persistenceId, entityState1)

    // Act: save the second event, which will trigger a snapshot save and event deletion.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)
    persistenceTestKit.expectNextPersisted(persistenceId, entityState2)

    // Assert:
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)
    assertForDuration(
      {
        assert(persistenceTestKit.persistedInStorage(persistenceId) === Seq(entityState1, entityState2))
      },
      max = remainingOrDefault,
    )
  }

  "log a warning message if an event deletion fails" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 2,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldEvents = true,
      ),
    )

    // Arrange: save the first event.
    locally {
      val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
      saveEntitySnapshot(snapshotStore, entityState1)
    }

    // Act & Assert:
    LoggingTestKit.warn("Failed to deleteMessages").expect {
      persistenceTestKit.failNextDelete(persistenceId)
      // Act: save the second event, which will trigger a snapshot save and event deletion.
      val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
      saveEntitySnapshot(snapshotStore, entityState2)
    }
  }

  "not delete snapshots if snapshot deletion is disabled" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 1,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldSnapshots = false,
      ),
    )

    // Arrange: save the first snapshot.
    val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
    saveEntitySnapshot(snapshotStore, entityState1)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState1)

    // Arrange: save the second snapshot.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)

    // Act: save the third snapshot, which will not trigger a snapshot deletion.
    val entityState3 = createEntitySnapshotData(LogEntryIndex(3))
    saveEntitySnapshot(snapshotStore, entityState3)

    // Assert:
    snapshotTestKit.expectNextPersisted(persistenceId, entityState3)
    assertForDuration(
      {
        val allSnapshots = Seq(entityState1, entityState2, entityState3)
        assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === allSnapshots)
      },
      max = remainingOrDefault,
    )
  }

  "not delete snapshots if it fails a snapshot save" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 1,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldSnapshots = true,
      ),
    )

    // Arrange: save the first snapshot.
    val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
    saveEntitySnapshot(snapshotStore, entityState1)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState1)

    // Arrange: save the second snapshot.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)

    LoggingTestKit.warn("Failed to saveSnapshot").expect {
      // Arrange: the next snapshot save will fail.
      snapshotTestKit.failNextPersisted(persistenceId)
      // Act: save the third snapshot, which will trigger a snapshot save.
      val entityState3 = createEntitySnapshotData(LogEntryIndex(3))
      saveEntitySnapshot(snapshotStore, entityState3)
    }

    // Assert:
    assertForDuration(
      {
        val allSnapshots = Seq(entityState1, entityState2)
        assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === allSnapshots)
      },
      max = remainingOrDefault,
    )
  }

  "delete snapshots matching the criteria if it successfully saves a snapshot" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 1,
        deleteBeforeRelativeSequenceNr = 1,
        deleteOldSnapshots = true,
      ),
    )

    // Arrange: save the first snapshot.
    locally {
      val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
      saveEntitySnapshot(snapshotStore, entityState1)
      snapshotTestKit.expectNextPersisted(persistenceId, entityState1)
    }

    // Arrange: save the second snapshot.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)

    // Act: save the third snapshot, which will trigger a snapshot deletion.
    val entityState3 = createEntitySnapshotData(LogEntryIndex(3))
    saveEntitySnapshot(snapshotStore, entityState3)

    // Assert:
    awaitAssert {
      assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === Seq(entityState2, entityState3))
    }
  }

  "not delete snapshots if it successfully saves a snapshot, but no snapshots match the criteria" in new Fixture {
    val snapshotStore = spawnSnapshotStore(
      configFor(
        snapshotEvery = 1,
        deleteBeforeRelativeSequenceNr = 10,
        deleteOldSnapshots = true,
      ),
    )

    // Arrange: save the first snapshot.
    val entityState1 = createEntitySnapshotData(LogEntryIndex(1))
    saveEntitySnapshot(snapshotStore, entityState1)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState1)

    // Arrange: save the second snapshot.
    val entityState2 = createEntitySnapshotData(LogEntryIndex(2))
    saveEntitySnapshot(snapshotStore, entityState2)
    snapshotTestKit.expectNextPersisted(persistenceId, entityState2)

    // Act: save the third snapshot, which will trigger a snapshot deletion.
    val entityState3 = createEntitySnapshotData(LogEntryIndex(3))
    saveEntitySnapshot(snapshotStore, entityState3)

    // Assert:
    snapshotTestKit.expectNextPersisted(persistenceId, entityState3)
    assertForDuration(
      {
        val allSnapshots = Seq(entityState1, entityState2, entityState3)
        assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === allSnapshots)
      },
      max = remainingOrDefault,
    )
  }

  "log a warning message if a snapshot deletion fails" ignore {
    // TODO: Write this test after `SnapshotTestKit.failNextDelete` comes to trigger a snapshot deletion failure.
    //   `SnapshotTestKit.failNextDelete` doesn't trigger a snapshot deletion failure at the time of writing.
    //   While underlying implementation `PersistenceTestKitSnapshotPlugin.deleteAsync` is supposed to return a failed
    //   Future, it always returns a successful Future.
  }

}
