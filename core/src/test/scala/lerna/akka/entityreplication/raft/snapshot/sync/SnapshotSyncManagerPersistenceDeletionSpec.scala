package lerna.akka.entityreplication.raft.snapshot.sync

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ typed, ActorRef, ActorSystem }
import akka.persistence.journal.Tagged
import akka.persistence.query.Offset
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.persistence.testkit.{ PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin, SnapshotMeta }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.persistence.EntitySnapshotsUpdatedTag
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager._
import lerna.akka.entityreplication.raft.snapshot.{ ShardSnapshotStore, SnapshotStore }
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{ BeforeAndAfterAll, Inside }

import java.util.UUID

object SnapshotSyncManagerPersistenceDeletionSpec {

  def config: Config = {
    PersistenceTestKitPlugin.config
      .withFallback(PersistenceTestKitSnapshotPlugin.config)
      .withFallback(raftPersistenceConfig)
      .withFallback(ConfigFactory.load())
  }

  private val raftPersistenceConfig: Config = ConfigFactory.parseString(
    s"""
       |lerna.akka.entityreplication.raft.persistence {
       |  journal.plugin = ${PersistenceTestKitPlugin.PluginId}
       |  snapshot-store.plugin = ${PersistenceTestKitSnapshotPlugin.PluginId}
       |  query.plugin = ${PersistenceTestKitReadJournal.Identifier}
       |}
       |""".stripMargin,
  )

}

final class SnapshotSyncManagerPersistenceDeletionSpec
    extends TestKit(
      ActorSystem(
        "SnapshotSyncManagerPersistenceDeletionSpec",
        SnapshotSyncManagerPersistenceDeletionSpec.config,
      ),
    )
    with ActorSpec
    with BeforeAndAfterAll
    with Inside
    with TypeCheckedTripleEquals {

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
      deleteBeforeRelativeSequenceNumber: Long,
      deleteOldEvents: Boolean = false,
      deleteOldSnapshots: Boolean = false,
  ): Config = {
    ConfigFactory
      .parseString(
        s"""
         |lerna.akka.entityreplication.raft.snapshot-sync {
         |  snapshot-copying-parallelism = 1
         |  delete-old-events = $deleteOldEvents
         |  delete-old-snapshots = $deleteOldSnapshots
         |  delete-before-relative-sequence-nr = $deleteBeforeRelativeSequenceNumber
         |}
         |""".stripMargin,
      ).withFallback(system.settings.config)
  }

  private trait Fixture {
    val shardId: NormalizedShardId  = NormalizedShardId.from(s"shard:${UUID.randomUUID().toString}")
    val typeName: TypeName          = TypeName.from(s"type-name:${UUID.randomUUID().toString}")
    val srcMemberIndex: MemberIndex = MemberIndex(s"member-index:${UUID.randomUUID().toString}")
    val dstMemberIndex: MemberIndex = MemberIndex(s"member-index:${UUID.randomUUID().toString}")

    val persistenceId: String =
      SnapshotSyncManager.persistenceId(typeName, srcMemberIndex, dstMemberIndex, shardId)
    val destinationShardSnapshotStorePersistenceId: String =
      s"shard-snapshot-store:${UUID.randomUUID().toString}"
    val raftActorPersistenceId: String =
      s"raft-???:${UUID.randomUUID().toString}"

    val probe: TestProbe = TestProbe()

    def spawnSnapshotSyncManager(config: Config): ActorRef =
      spawnSnapshotSyncManager(RaftSettings(config))

    def spawnSnapshotSyncManager(settings: RaftSettings): ActorRef = {
      val dstSnapshotStore = planAutoKill {
        system.actorOf(
          ShardSnapshotStore.props(typeName, settings, dstMemberIndex),
          destinationShardSnapshotStorePersistenceId,
        )
      }
      planAutoKill {
        system.actorOf(
          SnapshotSyncManager.props(
            typeName,
            srcMemberIndex,
            dstMemberIndex,
            dstSnapshotStore,
            shardId,
            settings,
          ),
        )
      }
    }

    val entityId: NormalizedEntityId =
      NormalizedEntityId.from(s"entity:${UUID.randomUUID().toString}")

    /** New events `runEntitySnapshotSynchronization` saves
      *
      * For clarification of test cases. This value should be updated if the implementation of
      * `runEntitySnapshotSynchronization` changes. This value change doesn't affect `runEntitySnapshotSynchronization` behavior.
      */
    val NewEventsSynchronizationSaves: Seq[SnapshotSyncManager.Event] = Seq(
      SnapshotCopied(Offset.sequence(1), dstMemberIndex, shardId, Term(1), LogEntryIndex(2), Set(entityId)),
      SyncCompleted(Offset.sequence(1)),
    )

    /** New snapshots `runEntitySnapshotSynchronization` saves
      *
      * For clarification of test cases. This value should be updated if the implementation of
      * `runEntitySnapshotSynchronization` changes. This value change doesn't affect `runEntitySnapshotSynchronization` behavior.
      */
    val NewSnapshotSynchronizationSaves: SnapshotSyncManager.State =
      SyncProgress(Offset.sequence(1))

    def runEntitySnapshotSynchronization(snapshotSyncManager: ActorRef): Unit = {
      // Require: events and snapshots of the SnapshotSyncManager should have no offset.
      // PersistenceTestKitReadJournal.currentEventsByTag that the SnapshotSyncManager runs has yet to support offsets.
      persistenceTestKit.persistedInStorage(persistenceId).foreach { event =>
        inside(event) {
          case SyncCompleted(offset) =>
            require(
              offset === Offset.noOffset,
              "SyncCompleted.offset should be NoOffset since PersistenceTestKitReadJournal has yet to support offsets.",
            )
        }
      }
      snapshotTestKit.persistedInStorage(persistenceId).foreach {
        case (_, snapshot) =>
          inside(snapshot) {
            case SyncProgress(offset) =>
              require(
                offset === Offset.noOffset,
                "SyncProgress.offset should be NoOffset since PersistenceTestKitReadJournal has yet to support offsets.",
              )
          }
      }

      // Prepare: source entity snapshots
      persistenceTestKit.persistForRecovery(
        SnapshotStore.persistenceId(typeName, entityId, srcMemberIndex),
        Seq(EntitySnapshot(EntitySnapshotMetadata(entityId, LogEntryIndex(2)), EntityState("entity-1-state"))),
      )
      // Prepare: tagged events the SnapshotSyncManager subscribes
      persistenceTestKit.persistForRecovery(
        raftActorPersistenceId,
        Seq(
          Tagged(
            CompactionCompleted(srcMemberIndex, shardId, Term(1), LogEntryIndex(2), Set(entityId)),
            Set(EntitySnapshotsUpdatedTag(srcMemberIndex, shardId).toString),
          ),
        ),
      )

      // Trigger entity snapshot synchronization.
      snapshotSyncManager ! SnapshotSyncManager.SyncSnapshot(
        srcLatestSnapshotLastLogTerm = Term(1),
        srcLatestSnapshotLastLogIndex = LogEntryIndex(2),
        dstLatestSnapshotLastLogTerm = Term(1),
        dstLatestSnapshotLastLogIndex = LogEntryIndex(1),
        replyTo = probe.ref,
      )
      // Ensure the entity snapshot synchronization completes.
      probe.expectMsg(
        SnapshotSyncManager.SyncSnapshotSucceeded(Term(1), LogEntryIndex(2), srcMemberIndex),
      )

      // The entity snapshot synchronization result in two event saves and one snapshot save.
      // See: NewEventsSynchronizationSaves and NewSnapshotSynchronizationSaves
    }

  }

  "SnapshotSyncManager" should {

    "not delete events if event deletion is disabled" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 1,
        deleteOldEvents = false,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      val initialEvents = Seq(
        SyncCompleted(Offset.noOffset),
        SyncCompleted(Offset.noOffset),
      )
      persistenceTestKit.persistForRecovery(persistenceId, initialEvents)
      snapshotTestKit.persistForRecovery(
        persistenceId,
        SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
      )
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))

      // Act: run entity snapshot synchronization, which will trigger a snapshot save but not event deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      snapshotTestKit.expectNextPersisted(persistenceId, NewSnapshotSynchronizationSaves)
      assertForDuration(
        {
          val eventsAfterSynchronization = initialEvents ++ NewEventsSynchronizationSaves
          assert(persistenceTestKit.persistedInStorage(persistenceId) === eventsAfterSynchronization)
        },
        max = remainingOrDefault,
      )
    }

    "not delete events if it fails a snapshot save" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 1,
        deleteOldEvents = true,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      val initialEvents = Seq(
        SyncCompleted(Offset.noOffset),
        SyncCompleted(Offset.noOffset),
      )
      persistenceTestKit.persistForRecovery(persistenceId, initialEvents)
      snapshotTestKit.persistForRecovery(
        persistenceId,
        SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
      )
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))

      LoggingTestKit.warn("Failed to saveSnapshot").expect {
        // Arrange: the next snapshot save will fail.
        snapshotTestKit.failNextPersisted(persistenceId)
        // Act: run entity snapshot synchronization, which will trigger a snapshot save.
        val snapshotSyncManager = spawnSnapshotSyncManager(config)
        runEntitySnapshotSynchronization(snapshotSyncManager)
      }

      // Assert:
      assertForDuration(
        {
          val eventsAfterSynchronization = initialEvents ++ NewEventsSynchronizationSaves
          assert(persistenceTestKit.persistedInStorage(persistenceId) === eventsAfterSynchronization)
        },
        max = remainingOrDefault,
      )
    }

    "delete events matching the criteria if it successfully saves a snapshot" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 1,
        deleteOldEvents = true,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      persistenceTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
        ),
      )
      snapshotTestKit.persistForRecovery(
        persistenceId,
        SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
      )

      // Act: run entity snapshot synchronization, which will trigger a snapshot save and event deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      awaitAssert {
        val eventsAfterDelete = NewEventsSynchronizationSaves.lastOption.toSeq
        assert(persistenceTestKit.persistedInStorage(persistenceId) === eventsAfterDelete)
      }
    }

    "not delete events if it successfully saves a snapshot, but no events match the criteria" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 10,
        deleteOldEvents = true,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      val initialEvents = Seq(
        SyncCompleted(Offset.noOffset),
        SyncCompleted(Offset.noOffset),
      )
      persistenceTestKit.persistForRecovery(persistenceId, initialEvents)
      snapshotTestKit.persistForRecovery(
        persistenceId,
        SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
      )
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))

      // Act: run entity snapshot synchronization, which will trigger a snapshot save and event deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      snapshotTestKit.expectNextPersisted(persistenceId, NewSnapshotSynchronizationSaves)
      assertForDuration(
        {
          val eventsAfterSynchronization = initialEvents ++ NewEventsSynchronizationSaves
          assert(persistenceTestKit.persistedInStorage(persistenceId) === eventsAfterSynchronization)
        },
        max = remainingOrDefault,
      )
    }

    "log a warning message if an event deletion fails" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 1,
        deleteOldEvents = true,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      persistenceTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
        ),
      )
      snapshotTestKit.persistForRecovery(
        persistenceId,
        SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
      )

      // Act & Assert:
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      LoggingTestKit.warn("Failed to deleteMessages").expect {
        persistenceTestKit.failNextDelete(persistenceId)
        // Act: run entity snapshot synchronization, which will trigger a snapshot save and event deletion.
        runEntitySnapshotSynchronization(snapshotSyncManager)
      }
    }

    "not delete snapshots if snapshot deletion is disabled" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 1,
        deleteOldSnapshots = false,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      persistenceTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
        ),
      )
      snapshotTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
          SnapshotMeta(sequenceNr = 4) -> SyncProgress(Offset.noOffset),
        ),
      )
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))

      // Act: run entity snapshot synchronization, which will trigger a snapshot save but not deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      snapshotTestKit.expectNextPersisted(persistenceId, NewSnapshotSynchronizationSaves)
      assertForDuration(
        {
          val snapshotsAfterSynchronization = Seq(
            SyncProgress(Offset.noOffset),
            SyncProgress(Offset.noOffset),
            NewSnapshotSynchronizationSaves,
          )
          assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === snapshotsAfterSynchronization)
        },
        max = remainingOrDefault,
      )
    }

    "not delete snapshots if it fails a snapshot save" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 1,
        deleteOldSnapshots = true,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      persistenceTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
        ),
      )
      snapshotTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
          SnapshotMeta(sequenceNr = 4) -> SyncProgress(Offset.noOffset),
        ),
      )
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))

      LoggingTestKit.warn("Failed to saveSnapshot").expect {
        // Arrange: the next snapshot will fail.
        snapshotTestKit.failNextPersisted(persistenceId)
        // Act: run entity snapshot synchronization, which will trigger a snapshot save.
        val snapshotSyncManager = spawnSnapshotSyncManager(config)
        runEntitySnapshotSynchronization(snapshotSyncManager)
      }

      // Assert:
      assertForDuration(
        {
          val snapshotsAfterSynchronization = Seq(SyncProgress(Offset.noOffset), SyncProgress(Offset.noOffset))
          assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === snapshotsAfterSynchronization)
        },
        max = remainingOrDefault,
      )
    }

    "delete snapshots matching the criteria if it successfully saves a snapshot" in new Fixture {
      val settings = RaftSettings(
        configFor(
          deleteBeforeRelativeSequenceNumber = 1,
          deleteOldSnapshots = true,
        ),
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      persistenceTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
        ),
      )
      snapshotTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
        ),
      )

      // Act: run entity snapshot synchronization, which will trigger a snapshot save and deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(settings)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      awaitAssert {
        assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === Seq(NewSnapshotSynchronizationSaves))
      }
    }

    "not delete snapshots if it successfully saves a snapshot, but no snapshots match the criteria" in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 2,
        deleteOldSnapshots = true,
      )

      // Arrange: save events and snapshots of SnapshotSyncManager.
      persistenceTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SyncCompleted(Offset.noOffset),
          SyncCompleted(Offset.noOffset),
        ),
      )
      snapshotTestKit.persistForRecovery(
        persistenceId,
        Seq(
          SnapshotMeta(sequenceNr = 2) -> SyncProgress(Offset.noOffset),
        ),
      )
      snapshotTestKit.expectNextPersisted(persistenceId, SyncProgress(Offset.noOffset))

      // Act: run entity snapshot synchronization, which will trigger a snapshot and deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      snapshotTestKit.expectNextPersisted(persistenceId, NewSnapshotSynchronizationSaves)
      assertForDuration(
        {
          val snapshotsAfterSynchronization = Seq(
            SyncProgress(Offset.noOffset),
            NewSnapshotSynchronizationSaves,
          )
          assert(snapshotTestKit.persistedInStorage(persistenceId).map(_._2) === snapshotsAfterSynchronization)
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

    "stop after the synchronization completes if no deletions are enabled." in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 0,
        deleteOldEvents = false,
        deleteOldSnapshots = false,
      )

      // Act:
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      probe.watch(snapshotSyncManager)
      probe.expectTerminated(snapshotSyncManager)
    }

    "stop after the event deletion succeeds if only event deletion is enabled." in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 0,
        deleteOldEvents = true,
        deleteOldSnapshots = false,
      )

      // Act: run entity snapshot synchronization, which will trigger an event deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      probe.watch(snapshotSyncManager)
      probe.expectTerminated(snapshotSyncManager)
    }

    "stop after the event deletion fails if only event deletion is enabled." in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 0,
        deleteOldEvents = true,
        deleteOldSnapshots = false,
      )

      // Act: run entity snapshot synchronization, which will trigger en event deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      persistenceTestKit.failNextDelete(persistenceId)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      probe.watch(snapshotSyncManager)
      probe.expectTerminated(snapshotSyncManager)
    }

    "stop after the snapshot deletion succeeds if only snapshot deletion is enabled." in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 0,
        deleteOldEvents = false,
        deleteOldSnapshots = true,
      )

      // Act: run entity snapshot synchronization, which will trigger a snapshot deletion.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      probe.watch(snapshotSyncManager)
      probe.expectTerminated(snapshotSyncManager)
    }

    "stop after the snapshot deletion fails if only snapshot deletion is enabled." ignore {
      // TODO: Write this test after `SnapshotTestKit.failNextDelete` comes to trigger a snapshot deletion failure.
      //   `SnapshotTestKit.failNextDelete` doesn't trigger a snapshot deletion failure at the time of writing.
      //   While underlying implementation `PersistenceTestKitSnapshotPlugin.deleteAsync` is supposed to return a failed
      //   Future, it always returns a successful Future.
    }

    "stop after both deletions succeed if both deletions are enabled." in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 0,
        deleteOldEvents = true,
        deleteOldSnapshots = true,
      )

      // Act: run entity snapshot synchronization, which will trigger both deletions.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      probe.watch(snapshotSyncManager)
      probe.expectTerminated(snapshotSyncManager)
    }

    "stop after only the event deletion fails if both deletions are enabled." in new Fixture {
      val config = configFor(
        deleteBeforeRelativeSequenceNumber = 0,
        deleteOldEvents = true,
        deleteOldSnapshots = true,
      )

      // Act: run entity snapshot synchronization, which will trigger both deletions.
      val snapshotSyncManager = spawnSnapshotSyncManager(config)
      persistenceTestKit.failNextDelete(persistenceId)
      runEntitySnapshotSynchronization(snapshotSyncManager)

      // Assert:
      probe.watch(snapshotSyncManager)
      probe.expectTerminated(snapshotSyncManager)
    }

    "stop after only the snapshot deletion fails if both deletions are enabled." ignore {
      // TODO: Write this test after `SnapshotTestKit.failNextDelete` comes to trigger a snapshot deletion failure.
      //   `SnapshotTestKit.failNextDelete` doesn't trigger a snapshot deletion failure at the time of writing.
      //   While underlying implementation `PersistenceTestKitSnapshotPlugin.deleteAsync` is supposed to return a failed
      //   Future, it always returns a successful Future.
    }

    "stop after both deletions fail if both deletions are enabled." ignore {
      // TODO: Write this test after `SnapshotTestKit.failNextDelete` comes to trigger a snapshot deletion failure.
      //   `SnapshotTestKit.failNextDelete` doesn't trigger a snapshot deletion failure at the time of writing.
      //   While underlying implementation `PersistenceTestKitSnapshotPlugin.deleteAsync` is supposed to return a failed
      //   Future, it always returns a successful Future.
    }

  }

}
