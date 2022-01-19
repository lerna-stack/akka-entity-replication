package lerna.akka.entityreplication.typed

import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.event.Logging
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.persistence.query.scaladsl.CurrentEventsByTagQuery
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.util.AtLeastOnceComplete
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object EventSourcingAutoStartMultiNodeSpecConfig extends MultiNodeConfig {
  // node1~3 form the first cluster.
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")

  // node4~6 form the second cluster after the first cluster stops.
  val node4: RoleName = role("node4")
  val node5: RoleName = role("node5")
  val node6: RoleName = role("node6")

  private val testConfig: Config =
    ConfigFactory.parseString(s"""
        |lerna.akka.entityreplication.raft.multi-raft-roles = ["member-1", "member-2", "member-3"]
        |lerna.akka.entityreplication.recovery-entity-timeout = 1s
        |
        |# Overwrite to speed up tests.
        |# All Raft actors will start immediately after a cluster startup.
        |lerna.akka.entityreplication {
        |  raft {
        |    number-of-shards = 3
        |    raft-actor-auto-start {
        |      number-of-actors = 3
        |    }
        |  }
        |}
        |
        |inmemory-journal {
        |  event-adapters {
        |    catalog-tagging = "lerna.akka.entityreplication.typed.EventSourcingAutoStartMultiNodeSpec$$CatalogEventAdapter"
        |  }
        |  event-adapter-bindings {
        |    "lerna.akka.entityreplication.typed.EventSourcingAutoStartMultiNodeSpec$$Catalog$$Event" = catalog-tagging
        |  }
        |}
        |""".stripMargin)

  commonConfig(
    debugConfig(false)
      .withFallback(testConfig)
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )

  // Use invalid plugins to emulate journal failures.
  private val invalidEventSourcedPersistenceConfig =
    ConfigFactory.parseString("""
        |lerna.akka.entityreplication.raft.eventsourced.persistence {
        |  journal.plugin = invalid
        |  snapshot-store.plugin = invalid
        |}
        |""".stripMargin)

  // node1~3 fail to persist events on the read side.
  nodeConfig(node1)(
    ConfigFactory
      .parseString(s"""akka.cluster.roles = ["member-1"]""")
      .withFallback(invalidEventSourcedPersistenceConfig),
  )
  nodeConfig(node2)(
    ConfigFactory
      .parseString(s"""akka.cluster.roles = ["member-2"]""")
      .withFallback(invalidEventSourcedPersistenceConfig),
  )
  nodeConfig(node3)(
    ConfigFactory
      .parseString(s"""akka.cluster.roles = ["member-3"]""")
      .withFallback(invalidEventSourcedPersistenceConfig),
  )

  // node4~6 succeed to persist events on the read side.
  nodeConfig(node4)(
    ConfigFactory
      .parseString(s"""akka.cluster.roles = ["member-1"]"""),
  )
  nodeConfig(node5)(
    ConfigFactory
      .parseString(s"""akka.cluster.roles = ["member-2"]"""),
  )
  nodeConfig(node6)(
    ConfigFactory
      .parseString(s"""akka.cluster.roles = ["member-3"]"""),
  )

}

final class EventSourcingAutoStartMultiNodeSpecMultiJvmNode1 extends EventSourcingAutoStartMultiNodeSpec
final class EventSourcingAutoStartMultiNodeSpecMultiJvmNode2 extends EventSourcingAutoStartMultiNodeSpec
final class EventSourcingAutoStartMultiNodeSpecMultiJvmNode3 extends EventSourcingAutoStartMultiNodeSpec

final class EventSourcingAutoStartMultiNodeSpecMultiJvmNode4 extends EventSourcingAutoStartMultiNodeSpec
final class EventSourcingAutoStartMultiNodeSpecMultiJvmNode5 extends EventSourcingAutoStartMultiNodeSpec
final class EventSourcingAutoStartMultiNodeSpecMultiJvmNode6 extends EventSourcingAutoStartMultiNodeSpec

class EventSourcingAutoStartMultiNodeSpec
    extends MultiNodeSpec(EventSourcingAutoStartMultiNodeSpecConfig)
    with STMultiNodeSpec {

  import EventSourcingAutoStartMultiNodeSpec._
  import EventSourcingAutoStartMultiNodeSpecConfig._

  override def initialParticipants: Int = 3

  /** ClusterReplication should be ready to handle requests whitin this timeout */
  val initializationTimeout: FiniteDuration = 10.seconds

  /** ClusterReplication should persist events eventually within this timeout. */
  val propagationTimeout: FiniteDuration = 10.seconds

  private implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
  private val clusterReplication: ClusterReplication     = ClusterReplication(typedSystem)
  private val readJournal: CurrentEventsByTagQuery =
    PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](
      readJournalPluginId = "lerna.akka.entityreplication.util.persistence.query.proxy",
    )

  "Cluster Replication Event Sourcing" should {

    "start a new cluster ([node1,node2,node3])" in {
      joinCluster(node1, node2, node3)
    }

    "initialize ClusterReplication on [node1,node2,node3]" in {
      runOn(node1, node2, node3) {
        clusterReplication.init(Catalog(typedSystem))
      }
      enterBarrier("Done ClusterReplication initialization")
    }

    "persist events via node1" in {
      runOn(node1) {
        // Use the initialization timeout as a Ask Timeout
        // since it is not ensured that the ClusterReplication can handle user requests immediately.
        implicit val timeout: Timeout     = initializationTimeout
        val retryInterval: FiniteDuration = 200.millis

        val entityRef = clusterReplication.entityRefFor(Catalog.typeKey, "example-1")
        val reply1    = AtLeastOnceComplete.askTo(entityRef, Catalog.Add("1", _), retryInterval).await
        reply1 shouldBe Catalog.AddReply(Set("1"))
        val reply2 = AtLeastOnceComplete.askTo(entityRef, Catalog.Add("2", _), retryInterval).await
        reply2 shouldBe Catalog.AddReply(Set("2", "1"))
      }
      enterBarrier("Done event persists")
    }

    "not read persisted events using PersistenceQuery on [node1,node2,nod3]" in {
      // Events is not available since `lerna.akka.entityreplication.raft.eventsourced.persistence` is invalid.
      runOn(node1, node2, node3) {
        assertForDuration(
          {
            val source = readJournal
              .currentEventsByTag(CatalogEventAdapter.tag, Offset.noOffset)
            val events = source.runFold(Seq.empty[EventEnvelope])(_ :+ _).await.collect {
              case EventEnvelope(_, _, _, event: Catalog.Event) => event
            }
            events shouldBe Seq.empty
          },
          propagationTimeout,
        )
      }
      enterBarrier("Done persisted-event reads on [node1,node2,node3]")
    }

    // NOTE: Starting a new cluster is crucial for this test.
    //       This test verifies Event Sourcing will automatically start without any interaction from users.
    "stop the cluster ([node1,node2,node3]) and then " in {
      leaveCluster(node1, node2, node3)
    }
    "start a new cluster ([node4,node5,node6])" in {
      newCluster(node4, node5, node6)
    }

    "initialize ClusterReplication on [node4,node5,node6]" in {
      runOn(node4, node5, node6) {
        clusterReplication.init(Catalog(typedSystem))
      }
      enterBarrier("Done ClusterReplication initialization")
    }

    "read persisted events using PersistenceQuery on [node4,node5,node6]" in {
      runOn(node4, node5, node6) {
        awaitAssert(
          {
            val source = readJournal
              .currentEventsByTag(CatalogEventAdapter.tag, Offset.noOffset)
            val events = source.runFold(Seq.empty[EventEnvelope])(_ :+ _).await.collect {
              case EventEnvelope(_, _, _, event: Catalog.Event) => event
            }
            events shouldBe Seq(
              Catalog.Added("1"),
              Catalog.Added("2"),
            )
          },
          initializationTimeout + propagationTimeout,
        )
      }
      enterBarrier("Done persisted-event reads on [node4,node5,node6]")
    }

  }

}

object EventSourcingAutoStartMultiNodeSpec {

  /** Holds a set of string values */
  object Catalog {
    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey(s"catalog")

    sealed trait Command                                             extends STMultiNodeSerializable
    final case class Add(value: String, replyTo: ActorRef[AddReply]) extends Command
    final case class AddReply(values: Set[String])                   extends STMultiNodeSerializable

    sealed trait Event                    extends STMultiNodeSerializable
    final case class Added(value: String) extends Event

    final case class State(values: Set[String]) extends STMultiNodeSerializable

    def apply(
        system: ActorSystem[_],
    ): ReplicatedEntity[Command, ReplicationEnvelope[Command]] = {
      val settings = ClusterReplicationSettings(system)
      ReplicatedEntity(typeKey)(entityContext =>
        Behaviors.setup { context =>
          context.setLoggerName(Catalog.getClass)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(Set.empty),
            commandHandler = commandHandler,
            eventHandler = eventHandler,
          )
        },
      ).withSettings(settings)
    }

    // NOTE: Command Handler should be idempotent.
    def commandHandler(state: State, command: Command): Effect[Event, State] =
      command match {
        case Add(value, replyTo) =>
          if (state.values.contains(value)) {
            Effect.none.thenReply(replyTo) { _: State => AddReply(state.values) }
          } else {
            Effect
              .replicate(Added(value))
              .thenReply(replyTo)(newState => AddReply(newState.values))
          }
      }

    def eventHandler(state: State, event: Event): State =
      event match {
        case Added(value) =>
          state.copy(state.values + value)
      }

  }

  object CatalogEventAdapter {
    val tag = "catalog"
  }
  final class CatalogEventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {
    private val log                           = Logging(system, getClass)
    override def manifest(event: Any): String = ""
    override def toJournal(event: Any): Any =
      event match {
        case event: Catalog.Event =>
          Tagged(event, tags = Set(CatalogEventAdapter.tag))
        case _ =>
          log.warning("Got unexpected event [{}]", event)
          event
      }
  }

}
