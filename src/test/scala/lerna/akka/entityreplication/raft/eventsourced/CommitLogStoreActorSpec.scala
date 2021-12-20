package lerna.akka.entityreplication.raft.eventsourced

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ typed, ActorRef, ActorSystem }
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.ActorSpec
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }
import org.scalatest.BeforeAndAfterAll

import java.util.UUID

object CommitLogStoreActorSpec {

  val pluginConfig: Config = ConfigFactory.parseString(s"""
      |lerna.akka.entityreplication.raft.eventsourced.persistence.journal.plugin = ${PersistenceTestKitPlugin.PluginId}
      |""".stripMargin)

  def config: Config = {
    PersistenceTestKitPlugin.config
      .withFallback(pluginConfig)
      .withFallback(ConfigFactory.load())
  }

  type PersistenceId = String

}

final class CommitLogStoreActorSpec
    extends TestKit(ActorSystem("CommitLogStoreActorSpec", CommitLogStoreActorSpec.config))
    with ActorSpec
    with BeforeAndAfterAll {

  import CommitLogStoreActorSpec._

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
  private val typeName                                         = TypeName.from("CommitLogStoreActorSpec")
  private val persistenceTestKit                               = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
  }

  override def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  private def spawnCommitLogStoreActor(name: Option[String] = None): (ActorRef, NormalizedShardId, PersistenceId) = {
    val props         = CommitLogStoreActor.props(typeName, ClusterReplicationSettings.create(system))
    val actorName     = name.getOrElse(UUID.randomUUID().toString)
    val actor         = system.actorOf(props, actorName)
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

  }

}
