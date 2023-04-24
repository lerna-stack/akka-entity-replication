package lerna.akka.entityreplication.raft.eventsourced

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ typed, ActorRef, ActorSystem }
import akka.cluster.Cluster
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.persistence.testkit._
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.internal.ClusterReplicationSettingsImpl
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.ActorSpec
import lerna.akka.entityreplication.raft.model._
import org.scalatest.{ BeforeAndAfterAll, OptionValues }

import java.util.UUID
import scala.annotation.nowarn

object CommitLogStoreActorSpec {

  val commitLogStoreConfig: Config = ConfigFactory.parseString(s"""
      |lerna.akka.entityreplication.raft.eventsourced.persistence {
      |  journal.plugin = ${PersistenceTestKitPlugin.PluginId}
      |  snapshot-store.plugin = ${PersistenceTestKitSnapshotPlugin.PluginId}
      |}
      |""".stripMargin)

  def config: Config = {
    PersistenceTestKitPlugin.config
      .withFallback(PersistenceTestKitSnapshotPlugin.config)
      .withFallback(commitLogStoreConfig)
      .withFallback(ConfigFactory.load())
  }

  type PersistenceId = String

  private final class FailPersistedAfterNPersisted(n: Int) extends EventStorage.JournalPolicies.PolicyType {
    private var count: Int = 0
    override def tryProcess(persistenceId: String, processingUnit: JournalOperation): ProcessingResult = {
      processingUnit match {
        case WriteEvents(_) =>
          count += 1
          if (count <= n) ProcessingSuccess
          else StorageFailure()
        case _ => ProcessingSuccess
      }
    }
  }

}

@nowarn("msg=Use CommitLogStoreActor.AppendCommittedEntries instead.")
final class CommitLogStoreActorSpec
    extends TestKit(ActorSystem("CommitLogStoreActorSpec", CommitLogStoreActorSpec.config))
    with ActorSpec
    with BeforeAndAfterAll
    with OptionValues {

  import CommitLogStoreActor.{ AppendCommittedEntries, AppendCommittedEntriesResponse }
  import CommitLogStoreActorSpec._

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
  private val typeName                                         = TypeName.from("CommitLogStoreActorSpec")
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
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  private def configFor(
      snapshotEvery: Int,
      deleteOldEvents: Boolean = false,
      deleteOldSnapshots: Boolean = false,
      deleteBeforeRelativeSequenceNr: Long = 1000,
  ): Config = {
    ConfigFactory
      .parseString(
        s"""
           |lerna.akka.entityreplication.raft.eventsourced.persistence {
           |  snapshot-every = $snapshotEvery
           |  delete-old-events = $deleteOldEvents
           |  delete-old-snapshots = $deleteOldSnapshots
           |  delete-before-relative-sequence-nr = $deleteBeforeRelativeSequenceNr
           |}
           |""".stripMargin,
      ).withFallback(system.settings.config)
  }

  private def spawnCommitLogStoreActor(
      name: Option[String] = None,
      settings: ClusterReplicationSettings = ClusterReplicationSettings.create(system),
      beforeSpawn: PersistenceId => Unit = _ => {},
  ): (ActorRef, NormalizedShardId, PersistenceId) = {
    val props         = CommitLogStoreActor.props(typeName, settings)
    val actorName     = name.getOrElse(UUID.randomUUID().toString)
    val shardId       = NormalizedShardId.from(actorName)
    val persistenceId = CommitLogStoreActor.persistenceId(typeName, shardId.raw)
    beforeSpawn(persistenceId)
    val actor = planAutoKill(system.actorOf(props, actorName))
    (actor, shardId, persistenceId)
  }

  private def spawnCommitLogStoreActorWithConfig(
      config: Config,
      name: Option[String] = None,
      beforeSpawn: PersistenceId => Unit = _ => {},
  ): (ActorRef, NormalizedShardId, PersistenceId) = {
    val settings: ClusterReplicationSettings =
      ClusterReplicationSettingsImpl(config, Cluster(system).settings.Roles)
    spawnCommitLogStoreActor(name, settings, beforeSpawn)
  }

  "CommitLogStoreActor.AppendCommittedEntries" should {

    "throw an IllegalArgumentException if entries don't have monotonically increased indices" in {
      val shardId  = NormalizedShardId.from("1")
      val entityId = NormalizedEntityId.from("entity1")
      val exception = intercept[IllegalArgumentException] {
        AppendCommittedEntries(
          shardId,
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          ),
        )
      }
      assert(
        exception.getMessage ===
          "requirement failed: entries should have monotonically increased indices. expected: 2 (1+1), but got: 3",
      )
    }

  }

  "CommitLogStoreActor" should {

    "accept a Save command with an old LogEntryIndex, save no events, and reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val oldIndex                                      = LogEntryIndex.initial()
      commitLogStoreActor ! Save(shardId, oldIndex, NoOp)
      expectMsg(Done)
      persistenceTestKit.expectNothingPersisted(persistenceId)
    }

    "accept a Save command with the expected LogEntryIndex and NoOp, save an event, and reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val expectedIndex                                 = LogEntryIndex.initial().next()
      commitLogStoreActor ! Save(shardId, expectedIndex, NoOp)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)
    }

    "accept a Save command with the expected LogEntryIndex and a user domain event, save the user domain event, and reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val expectedIndex                                 = LogEntryIndex.initial().next()
      val domainEvent                                   = "User domain event"
      commitLogStoreActor ! Save(shardId, expectedIndex, domainEvent)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
    }

    "updates its state if it saves an event" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()

      val firstIndex = LogEntryIndex.initial().next()
      commitLogStoreActor ! Save(shardId, firstIndex, NoOp)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)

      val secondIndex  = firstIndex.next()
      val domainEvent1 = "User domain event 1"
      commitLogStoreActor ! Save(shardId, secondIndex, domainEvent1)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, domainEvent1)

      val thirdEvent   = secondIndex.next()
      val domainEvent2 = "User domain event 2"
      commitLogStoreActor ! Save(shardId, thirdEvent, domainEvent2)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, domainEvent2)
    }

    "deny a Save command with a newer LogEntryIndex than expected" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val newerIndex                                    = LogEntryIndex.initial().plus(2)
      commitLogStoreActor ! Save(shardId, newerIndex, NoOp)
      expectNoMessage()
      persistenceTestKit.expectNothingPersisted(persistenceId)
    }

    "stop if an event save fails" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val expectedIndex                                 = LogEntryIndex.initial().next()

      // The default implementation generates an error log.
      // It's great to verify that the error log was generated.
      val expectedErrorMessage =
        s"Failed to persist event type [${InternalEvent.getClass.getName}] with sequence number [1] for persistenceId [$persistenceId]"
      LoggingTestKit
        .error(expectedErrorMessage)
        .expect {
          persistenceTestKit.failNextPersisted()
          commitLogStoreActor ! Save(shardId, expectedIndex, NoOp)
        }
      expectNoMessage()

      watch(commitLogStoreActor)
      expectTerminated(commitLogStoreActor)
    }

    "stop if an event replay fails" in {
      val name          = UUID.randomUUID().toString
      val persistenceId = CommitLogStoreActor.persistenceId(typeName, name)

      // The default implementation generates an error log.
      // It's great to verify that the error log was generated.
      val expectedErrorMessage =
        s"Persistence failure when replaying events for persistenceId [$persistenceId]. Last known sequence number [0]"
      val (commitLogStoreActor, _, _) = LoggingTestKit
        .error(expectedErrorMessage)
        .expect {
          persistenceTestKit.failNextRead()
          spawnCommitLogStoreActor(Option(name))
        }

      watch(commitLogStoreActor)
      expectTerminated(commitLogStoreActor)
    }

    "save a snapshot every `snapshot-every` events" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)
      val indices                                       = Vector.tabulate(snapshotEvery * 2)(_ + 1)
      indices.foreach { i =>
        val index              = LogEntryIndex.initial().plus(i)
        val shouldSaveSnapshot = i % snapshotEvery == 0
        val domainEvent        = s"Event $i"
        if (shouldSaveSnapshot) {
          // The implementation should generate an info log.
          // It's great to verify that the info log was generated.
          LoggingTestKit.info("Succeeded to saveSnapshot").expect {
            commitLogStoreActor ! Save(shardId, index, domainEvent)
          }
          expectMsg(Done)
          persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
          snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(index))
        } else {
          commitLogStoreActor ! Save(shardId, index, domainEvent)
          expectMsg(Done)
          persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
        }
      }
    }

    "load the latest snapshot if it restarts" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val name                                          = UUID.randomUUID().toString
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config, Option(name))

      // Save a snapshot.
      def shouldSaveSnapshot(index: LogEntryIndex): Boolean = index.underlying % snapshotEvery == 0
      val indices                                           = Vector.tabulate(snapshotEvery + 1) { i => LogEntryIndex.initial().plus(i + 1) }
      indices.zipWithIndex.foreach {
        case (index, n) =>
          val domainEvent = s"User Domain Event $n"
          commitLogStoreActor ! Save(shardId, index, domainEvent)
          expectMsg(Done)
          persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
          if (shouldSaveSnapshot(index)) {
            snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(index))
          }
      }

      val latestSnapshotIndex = indices.filter(shouldSaveSnapshot).lastOption.value
      val latestIndex         = indices.lastOption.value
      // To verify that the actor will replay at-least one event, `latestIndex` should be greater than `latestSnapshotIndex`.
      assume(latestIndex > latestSnapshotIndex, "`snapshot-every` should be greater than 1.")

      // Stop the actor
      watch(commitLogStoreActor)
      system.stop(commitLogStoreActor)
      expectTerminated(commitLogStoreActor)

      // Restart the actor
      val (newCommitLogStoreActor, _, _) =
        LoggingTestKit.info(s"Loaded snapshot [State(${latestSnapshotIndex.underlying})]").expect {
          spawnCommitLogStoreActor(Option(name))
        }

      // The actor should save an event with the next of the latest index
      val index       = latestIndex.next()
      val domainEvent = "User Domain Event after restart"
      newCommitLogStoreActor ! Save(shardId, index, domainEvent)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
    }

    "continue it's behavior if a snapshot save fails" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)
      val indices                                       = Vector.tabulate(snapshotEvery) { i => LogEntryIndex.initial().plus(i + 1) }
      indices.zipWithIndex.dropRight(1).foreach {
        case (index, n) =>
          val domainEvent = s"Event $n"
          commitLogStoreActor ! Save(shardId, index, domainEvent)
          expectMsg(Done)
          persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
      }
      val snapshotIndex = indices.lastOption.value

      // The actor fails a snapshot save, but continues to accept next commands.
      // The implementation should generate a warn log.
      // It's great to verify that the warn log was generated.
      LoggingTestKit.warn("Failed to saveSnapshot").expect {
        snapshotTestKit.failNextPersisted()
        commitLogStoreActor ! Save(shardId, snapshotIndex, NoOp)
      }
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)
      snapshotTestKit.expectNothingPersisted(persistenceId)

      // The actor should handle a next Save command.
      val nextEventIndex = snapshotIndex.next()
      commitLogStoreActor ! Save(shardId, nextEventIndex, NoOp)
      expectMsg(Done)
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)

    }

    "stop if a snapshot replay fails" in {
      val name          = UUID.randomUUID().toString
      val persistenceId = CommitLogStoreActor.persistenceId(typeName, name)

      // The default implementation generates an error log.
      // It's great to verify that the error log was generated.
      val expectedErrorMessage =
        s"Persistence failure when replaying events for persistenceId [$persistenceId]. Last known sequence number [0]"
      val (commitLogStoreActor, _, _) = LoggingTestKit
        .error(expectedErrorMessage)
        .expect {
          snapshotTestKit.failNextRead()
          spawnCommitLogStoreActor(Option(name))
        }

      watch(commitLogStoreActor)
      expectTerminated(commitLogStoreActor)
    }

    "not save a snapshot if an event save fails" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)
      val indices                                       = Vector.tabulate(snapshotEvery) { i => LogEntryIndex.initial().plus(i + 1) }
      indices.zipWithIndex.dropRight(1).foreach {
        case (index, n) =>
          val domainEvent = s"Event $n"
          commitLogStoreActor ! Save(shardId, index, domainEvent)
          expectMsg(Done)
          persistenceTestKit.expectNextPersisted(persistenceId, domainEvent)
      }

      val snapshotIndex = indices.lastOption.value
      persistenceTestKit.failNextPersisted()
      commitLogStoreActor ! Save(shardId, snapshotIndex, NoOp)
      expectNoMessage()
      persistenceTestKit.expectNothingPersisted(persistenceId)
      snapshotTestKit.expectNothingPersisted(persistenceId)
    }

    "handle AppendCommittedEntries(entries=empty), persist no events, and then reply" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      commitLogStoreActor ! AppendCommittedEntries(shardId, Seq.empty)
      expectMsg(AppendCommittedEntriesResponse(LogEntryIndex(0)))
      persistenceTestKit.expectNothingPersisted(persistenceId)
    }

    "handle AppendCommittedEntries(entries=[1,2,3]), persist all events if its current index is 0, and then reply" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val entries = {
        val entityId = NormalizedEntityId.from("entity1")
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(LogEntryIndex(3)))
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)
      persistenceTestKit.expectNextPersisted(persistenceId, "event1")
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)
    }

    "handle AppendCommittedEntries(entries=[2,3,4]), persist no events if its current index is 0, and then reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
      val entries = {
        val entityId = NormalizedEntityId.from("entity1")
        Seq(
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event2"), Term(2)),
        )
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(LogEntryIndex(0)))
      persistenceTestKit.expectNothingPersisted(persistenceId)
    }

    "handle AppendCommittedEntries(entries=[1,2]), persist no events if its current index is 2, and then reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor(beforeSpawn = persistenceId => {
        val initialEvents: Seq[Any] = Seq(InternalEvent, "event1")
        persistenceTestKit.persistForRecovery(persistenceId, initialEvents)
      })
      val entries = {
        val entityId = NormalizedEntityId.from("entity1")
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
        )
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(LogEntryIndex(2)))
      persistenceTestKit.expectNothingPersisted(persistenceId)
    }

    "handle AppendCommittedEntries(entries=[1,2]), persist no events if its current index is 3, and then reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor(beforeSpawn = persistenceId => {
        val initialEvents: Seq[Any] = Seq(InternalEvent, "event1", "event2")
        persistenceTestKit.persistForRecovery(persistenceId, initialEvents)
      })
      val entries = {
        val entityId = NormalizedEntityId.from("entity1")
        Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
        )
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(LogEntryIndex(3)))
      persistenceTestKit.expectNothingPersisted(persistenceId)
    }

    "handle AppendCommittedEntries(entries=[2,3,4,5,6]), persist only not-persisted events, and then reply to the command" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor(beforeSpawn = persistenceId => {
        val initialEvents: Seq[Any] = Seq(InternalEvent, "event1", "event2")
        persistenceTestKit.persistForRecovery(persistenceId, initialEvents)
      })
      val entries = {
        val entityId = NormalizedEntityId.from("entity1")
        Seq(
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event3"), Term(2)),
          LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event4"), Term(2)),
        )
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(LogEntryIndex(6)))
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)
      persistenceTestKit.expectNextPersisted(persistenceId, "event3")
      persistenceTestKit.expectNextPersisted(persistenceId, "event4")
    }

    "stop if an event save fails (with AppendCommittedEntries)" in {
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()

      // The default implementation generates an error log.
      // It's great to verify that the error log was generated.
      LoggingTestKit
        .error("Failed to persist event type")
        .expect {
          persistenceTestKit.withPolicy(new FailPersistedAfterNPersisted(1))
          val entries = {
            val entityId = NormalizedEntityId.from("entity1")
            Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(2)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
            )
          }
          commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
        }
      expectNoMessage()
      assert(persistenceTestKit.persistedInStorage(persistenceId) == Seq(InternalEvent))

      watch(commitLogStoreActor)
      expectTerminated(commitLogStoreActor)
    }

    "save a snapshot every `snapshot-every` events (with AppendCommittedEntries)" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      def makeEntries(baseIndex: LogEntryIndex, numberOfEntries: Int): Seq[LogEntry] = {
        val indices = Vector.tabulate(numberOfEntries)(i => baseIndex.plus(i))
        indices.map { index =>
          val entityId    = NormalizedEntityId("entity1")
          val domainEvent = s"Event with $index"
          LogEntry(index, EntityEvent(Option(entityId), domainEvent), term = Term(1))
        }
      }

      val firstEntries               = makeEntries(LogEntryIndex(1), snapshotEvery)
      val expectedFirstSnapshotIndex = LogEntryIndex(snapshotEvery)
      commitLogStoreActor ! AppendCommittedEntries(shardId, firstEntries)
      expectMsg(AppendCommittedEntriesResponse(firstEntries.last.index))
      firstEntries.foreach { entry =>
        persistenceTestKit.expectNextPersisted(persistenceId, entry.event.event)
      }
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(expectedFirstSnapshotIndex))

      val secondEntries               = makeEntries(LogEntryIndex(snapshotEvery + 1), snapshotEvery + 1)
      val expectedSecondSnapshotIndex = LogEntryIndex(snapshotEvery * 2)
      assume(
        secondEntries.last.index > expectedSecondSnapshotIndex,
        "To verify that a snapshot will be taken immediately not depending on the number of entries of AppendCommittedEntries",
      )
      commitLogStoreActor ! AppendCommittedEntries(shardId, secondEntries)
      expectMsg(AppendCommittedEntriesResponse(secondEntries.last.index))
      secondEntries.foreach { entry =>
        persistenceTestKit.expectNextPersisted(persistenceId, entry.event.event)
      }
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(expectedSecondSnapshotIndex))
    }

    "continue its behavior if a snapshot save fails (with AppendCommittedEntries)" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(snapshotEvery + 1) { i =>
        val entityId    = NormalizedEntityId("entity1")
        val index       = LogEntryIndex(1 + i) // 1-based indexing
        val domainEvent = s"Event $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      // The actor fails a snapshot save, but continues to accept next commands.
      // The implementation should generate a warn log.
      // It's great to verify that the warn log was generated.
      LoggingTestKit.warn("Failed to saveSnapshot").expect {
        snapshotTestKit.failNextPersisted()
        commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      }
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))
      entries.foreach { entry =>
        persistenceTestKit.expectNextPersisted(persistenceId, entry.event.event)
      }

      // The actor should handle a next command.
      val nextCommand = {
        val nextIndex   = entries.last.index.next()
        val nextEntries = Seq(LogEntry(nextIndex, EntityEvent(None, NoOp), Term(2)))
        AppendCommittedEntries(shardId, nextEntries)
      }
      commitLogStoreActor ! nextCommand
      expectMsg(AppendCommittedEntriesResponse(nextCommand.entries.last.index))
      persistenceTestKit.expectNextPersisted(persistenceId, InternalEvent)
    }

    "not save a snapshot if an event save fails (with AppendCommittedEntries)" in {
      val snapshotEvery                                 = 10
      val config                                        = configFor(snapshotEvery)
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(snapshotEvery) { i =>
        val entityId    = NormalizedEntityId("entity1")
        val index       = LogEntryIndex(1 + i) // 1-based indexing
        val domainEvent = s"Event $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }

      persistenceTestKit.withPolicy(new FailPersistedAfterNPersisted(snapshotEvery - 1))
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectNoMessage()
      entries.dropRight(1).foreach { entry =>
        persistenceTestKit.expectNextPersisted(persistenceId, entry.event.event)
      }
      persistenceTestKit.expectNothingPersisted(persistenceId)
      snapshotTestKit.expectNothingPersisted(persistenceId)
    }

    "stop itself when its shard id is defined as disabled" in {
      val settings =
        ClusterReplicationSettings
          .create(system)
          .withDisabledShards(Set("disabled-shard-id"))
      val (commitLogStoreActor, _, _) = spawnCommitLogStoreActor(name = Some("disabled-shard-id"), settings = settings)
      watch(commitLogStoreActor)
      expectTerminated(commitLogStoreActor)
    }

    "not delete events if event deletion is disabled" in {
      val config = configFor(
        snapshotEvery = 10,
        deleteOldEvents = false,
        deleteBeforeRelativeSequenceNr = 5,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(10) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))

      for (i <- 0 until 10) {
        persistenceTestKit.expectNextPersisted(persistenceId, s"Event with ${i + 1}")
      }
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(10)))
      assertForDuration(
        {
          assert(
            persistenceTestKit
              .persistedInStorage(persistenceId).size === 10,
          )
        },
        max = remainingOrDefault,
      )
    }

    "delete events matching the criteria if it successfully saves a snapshot" in {
      val config = configFor(
        snapshotEvery = 10,
        deleteOldEvents = true,
        deleteBeforeRelativeSequenceNr = 5,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(10) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))

      for (i <- 0 until 10) {
        persistenceTestKit.expectNextPersisted(persistenceId, s"Event with ${i + 1}")
      }
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(10)))
      awaitAssert {
        assert(
          persistenceTestKit
            .persistedInStorage(persistenceId).size === 10 - 5,
        )
      }
    }

    "not delete events if it successfully saves a snapshot, but no events match the criteria" in {
      val config = configFor(
        snapshotEvery = 10,
        deleteOldEvents = true,
        deleteBeforeRelativeSequenceNr = 11,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(10) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))

      for (i <- 0 until 10) {
        persistenceTestKit.expectNextPersisted(persistenceId, s"Event with ${i + 1}")
      }
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(10)))
      assertForDuration(
        {
          assert(
            persistenceTestKit
              .persistedInStorage(persistenceId).size === 10,
          )
        },
        max = remainingOrDefault,
      )
    }

    "continue its behavior if an event deletion fails" in {
      val config = configFor(
        snapshotEvery = 10,
        deleteOldEvents = true,
        deleteBeforeRelativeSequenceNr = 5,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(10) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      // The actor fails an event deletion but continues to accept the next commands.
      // The actor should log a warning for the deletion failure.
      LoggingTestKit.warn("Failed to deleteMessages").expect {
        persistenceTestKit.failNextDelete(persistenceId)
        commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
        expectMsg(AppendCommittedEntriesResponse(entries.last.index))
      }

      // The actor should handle the next command.
      val nextEntries = {
        val nextIndex = entries.last.index.next()
        Seq(LogEntry(nextIndex, EntityEvent(None, NoOp), Term(2)))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, nextEntries)
      expectMsg(AppendCommittedEntriesResponse(nextEntries.last.index))
    }

    "not delete snapshots if snapshot deletion is disabled" in {
      val config = configFor(
        snapshotEvery = 5,
        deleteOldSnapshots = false,
        deleteBeforeRelativeSequenceNr = 5,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(15) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))

      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(5)))
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(10)))
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(15)))
      assertForDuration(
        {
          assert(
            snapshotTestKit
              .persistedInStorage(persistenceId).size === 3,
          )
        },
        max = remainingOrDefault,
      )
    }

    "delete snapshots matching the criteria if it successfully saves a snapshot" in {
      val config = configFor(
        snapshotEvery = 5,
        deleteOldSnapshots = true,
        deleteBeforeRelativeSequenceNr = 5,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(15) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))

      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(5)))
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(10)))
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(15)))
      awaitAssert {
        assert(
          snapshotTestKit
            .persistedInStorage(persistenceId).size === 2,
        )
      }
    }

    "not delete snapshots if it successfully saves a snapshot, but no snapshots match the criteria" in {
      val config = configFor(
        snapshotEvery = 5,
        deleteOldSnapshots = true,
        deleteBeforeRelativeSequenceNr = 5,
      )
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActorWithConfig(config)

      val entries = Vector.tabulate(10) { i =>
        val index       = LogEntryIndex(i + 1)
        val entityId    = NormalizedEntityId("entity1")
        val domainEvent = s"Event with $index"
        LogEntry(index, EntityEvent(Option(entityId), domainEvent), Term(1))
      }
      commitLogStoreActor ! AppendCommittedEntries(shardId, entries)
      expectMsg(AppendCommittedEntriesResponse(entries.last.index))

      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(5)))
      snapshotTestKit.expectNextPersisted(persistenceId, CommitLogStoreActor.State(LogEntryIndex(10)))
      assertForDuration(
        {
          assert(
            snapshotTestKit
              .persistedInStorage(persistenceId).size === 2,
          )
        },
        max = remainingOrDefault,
      )
    }

    "continue its behavior if a snapshot deletion fails" ignore {
      // TODO: Write this test after `SnapshotTestKit.failNextDelete` comes to trigger a snapshot deletion failure.
      //   `SnapshotTestKit.failNextDelete` doesn't trigger a snapshot deletion failure at the time of writing.
      //   While underlying implementation `PersistenceTestKitSnapshotPlugin.deleteAsync` is supposed to return a failed
      //   Future, it always returns a successful Future.
    }
  }

}
