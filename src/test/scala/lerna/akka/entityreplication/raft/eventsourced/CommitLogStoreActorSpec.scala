package lerna.akka.entityreplication.raft.eventsourced

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ typed, ActorRef, ActorSystem }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.persistence.testkit.{ PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin }
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.ActorSpec
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }
import org.scalatest.{ BeforeAndAfterAll, OptionValues }

import java.util.UUID

object CommitLogStoreActorSpec {

  // Pick not too large and not too small value.
  //   * A too-large value requires more time to test.
  //   * A too-small value affects other tests unrelated to the snapshot feature.
  val snapshotEvery: Int = 10

  val commitLogStoreConfig: Config = ConfigFactory.parseString(s"""
      |lerna.akka.entityreplication.raft.eventsourced.persistence {
      |  journal.plugin = ${PersistenceTestKitPlugin.PluginId}
      |  snapshot-store.plugin = ${PersistenceTestKitSnapshotPlugin.PluginId}
      |  snapshot-every = $snapshotEvery
      |}
      |""".stripMargin)

  def config: Config = {
    PersistenceTestKitPlugin.config
      .withFallback(PersistenceTestKitSnapshotPlugin.config)
      .withFallback(commitLogStoreConfig)
      .withFallback(ConfigFactory.load())
  }

  type PersistenceId = String

}

final class CommitLogStoreActorSpec
    extends TestKit(ActorSystem("CommitLogStoreActorSpec", CommitLogStoreActorSpec.config))
    with ActorSpec
    with BeforeAndAfterAll
    with OptionValues {

  import CommitLogStoreActorSpec._

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
  private val typeName                                         = TypeName.from("CommitLogStoreActorSpec")
  private val persistenceTestKit                               = PersistenceTestKit(system)
  private val snapshotTestKit                                  = SnapshotTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
    snapshotTestKit.clearAll()
  }

  override def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  private def spawnCommitLogStoreActor(name: Option[String] = None): (ActorRef, NormalizedShardId, PersistenceId) = {
    val props         = CommitLogStoreActor.props(typeName, ClusterReplicationSettings.create(system))
    val actorName     = name.getOrElse(UUID.randomUUID().toString)
    val actor         = planAutoKill(system.actorOf(props, actorName))
    val shardId       = NormalizedShardId.from(actor.path)
    val persistenceId = CommitLogStoreActor.persistenceId(typeName, shardId.raw)
    (actor, shardId, persistenceId)
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
      assume(snapshotEvery > 0, "`snapshot-every` should be greater than 0.")
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
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
      // This test verify that an actor will replay the latest snapshot and at-least one event.
      assume(
        snapshotEvery > 1,
        "`snapshot-every` should be greater than 1 since an actor should replay at-least one event.",
      )

      val name                                          = UUID.randomUUID().toString
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor(Option(name))

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
      assume(snapshotEvery > 0, "`snapshot-every` should be greater than 0.")
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
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
      assume(snapshotEvery > 0, "`snapshot-every` should be greater than 0.")
      val (commitLogStoreActor, shardId, persistenceId) = spawnCommitLogStoreActor()
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

  }

}
