package lerna.akka.entityreplication.typed

import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, LoggingTestKit }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.pattern.StatusReply
import akka.remote.testconductor._
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.util.AtLeastOnceComplete
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")

  testTransport(on = true)

  private val testConfig: Config =
    ConfigFactory
      .parseString(s"""
                      |lerna.akka.entityreplication.raft.multi-raft-roles = [
                      |  "member-1", "member-2", "member-3"
                      |]
                      |lerna.akka.entityreplication {
                      |  # RaftActors will recover entities as possible quick.
                      |  recovery-entity-timeout = 2s
                      |  raft {
                      |    election-timeout = 1s
                      |    # EntityReplication runs only one RaftActor group for simplicity.
                      |    number-of-shards = 1
                      |    compaction {
                      |      log-size-check-interval = 25s
                      |      log-size-threshold = 10
                      |      preserve-log-size = 3
                      |    }
                      |  }
                      |}
                      |
                      |# LoggingTestKit uses this timeout.
                      |akka.actor.testkit.typed.filter-leeway = 30s
                      |
                      |# Sharding will be available as possible quick.
                      |akka.cluster.sharding {
                      |  retry-interval = 1s
                      |  waiting-for-state-timeout = 1s
                      |  updating-state-timeout = 1s
                      |  distributed-data.majority-min-cap = 2
                      |  coordinator-state.write-majority-plus = 0
                      |  coordinator-state.read-majority-plus = 0
                      |}
                      |""".stripMargin)

  commonConfig(
    debugConfig(false)
      .withFallback(testConfig)
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-1"]
          |""".stripMargin),
  )
  nodeConfig(node2)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-2"]
          |# RaftActor on this node won't become the leader for simplicity.
          |lerna.akka.entityreplication.raft.election-timeout = 1000s
          |""".stripMargin),
  )
  nodeConfig(node3)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-3"]
          |# RaftActor on this node won't become the leader for simplicity.
          |lerna.akka.entityreplication.raft.election-timeout = 1000s
          |""".stripMargin),
  )

}

final class ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpecMultiJvmNode1
    extends ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpec
final class ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpecMultiJvmNode2
    extends ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpec
final class ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpecMultiJvmNode3
    extends ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpec

/**
  * This test verifies that the entity should handle the new command only after the previous event (Raft log entry) has
  * been replicated and applied, even if compaction happens.
  *
  * This test uses network isolation to guarantee uncommitted log entries exist. However, it is no need to use network isolation,
  * and it is possible to use another approach. This test chose network isolation since it might be easy to stabilize this test.
  */
class ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpec
    extends MultiNodeSpec(ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpecConfig)
    with STMultiNodeSpec {

  import ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpec._
  import ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpecConfig._

  /** ClusterReplication should update its routing table within this timeout after cluster membership changes */
  private val routingTableUpdateTimeout: FiniteDuration = 10.seconds

  private implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
  private val clusterReplication: ClusterReplication     = ClusterReplication(typedSystem)

  private val testKit    = ActorTestKit(typedSystem)
  private val replyProbe = testKit.createTestProbe[StatusReply[Int]]()

  /** Verifies compaction completed by inspecting logging messages
    *
    * The compaction should be complete in the given block of code.
    */
  private def expectCompactionCompleted[T](code: => T): T = {
    LoggingTestKit.info("compaction completed").expect(code)
  }

  /** Wait for ClusterReplication to update its routing table */
  private def waitForRoutingTableUpdate(): Unit = {
    Thread.sleep(routingTableUpdateTimeout.toMillis)
  }

  private def entityRef(): ReplicatedEntityRef[Entity.Command] =
    clusterReplication.entityRefFor(Entity.TypeKey, entityId = "example-entity-1")

  private def putNextValueWithRetry(value: Int): Int = {
    implicit val timeout: Timeout = testKit.testKitSettings.SingleExpectDefaultTimeout
    AtLeastOnceComplete.askWithStatusTo(entityRef(), Entity.PutNext(value, _), 2.seconds).await
  }

  private def fetchValueWithRetry(): Int = {
    implicit val timeout: Timeout = testKit.testKitSettings.SingleExpectDefaultTimeout
    AtLeastOnceComplete.askTo(entityRef(), Entity.Get(_), 2.seconds).await
  }

  "A new cluster forms with nodes: [1,2,3]" in {
    newCluster(node1, node2, node3)
    clusterReplication.init(Entity(typedSystem))
  }

  "The leader (runs on node1) replicates log entries (these entries will be compacted)" in {
    runOn(node1) {
      putNextValueWithRetry(1) shouldBe 1
      (2 to 10).foreach { i =>
        putNextValueWithRetry(i) shouldBe i
      }
      // Sending a request and receiving a response without retry ensures no pending commands.
      val replyProbe = testKit.createTestProbe[Int]()
      entityRef() ! Entity.Get(replyProbe.ref)
      replyProbe.expectMessage(10)
    }
  }

  "Network isolation happens: [1],[2,3]" in {
    isolate(node1)
    // If there is no wait, ClusterReplication might try to deliver succeeding commands to the entity via another node than node1.
    waitForRoutingTableUpdate()
  }

  "The entity belonging to the leader (runs on node1) handles the new first command" in {
    runOn(node1) {
      LoggingTestKit.info("Replicating PutNextEvent(11) in State(10)").expect {
        entityRef() ! Entity.PutNext(11, replyProbe.ref)
      }
      // Replicating PutNextEvent(11) will be complete after network recovery
      // since RaftActor cannot replicate PutNextEvent(11) on the majority of members.
    }
  }

  "The leader (runs on node1) compacts its log entries" in {
    runOn(node1) {
      expectCompactionCompleted {
        // The leader has the uncommitted log entry; It compacts its committed (and applied) log entries.
        // This compaction should not affect the uncommitted log entry and ongoing log replication.
      }
    }
  }

  "The entity belonging to the leader (runs on node1) defers the new second command" in {
    runOn(node1) {
      // The leader will deliver this command to the entity, but the entity has to wait for the previous replication complete.
      // If the entity doesn't wait for the previous replication and handle this command, the entity will reply with an error.
      entityRef() ! Entity.PutNext(12, replyProbe.ref)
    }
  }

  "Network recovers from the isolation" in {
    releaseIsolation(node1)
    waitForRoutingTableUpdate()
  }

  "The entity belonging to the leader (runs on node1) continues replication and command handling. " +
  "It replies with success messages to the pending commands" in {
    runOn(node1) {
      replyProbe.expectMessage(StatusReply.Success(11))
      replyProbe.expectMessage(StatusReply.Success(12))
    }
  }

  "All nodes([1,2,3]) return the latest value" in {
    fetchValueWithRetry() shouldBe 12
  }

}

object ReplicatedEntityCommandHandlingBeforeAndAfterCompactionSpec {

  object Entity {
    val TypeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey(s"entity")
    val InitialValue                              = 0

    sealed trait Command                                                      extends STMultiNodeSerializable
    final case class PutNext(value: Int, replyTo: ActorRef[StatusReply[Int]]) extends Command
    final case class Get(replyTo: ActorRef[Int])                              extends Command

    sealed trait Event                        extends STMultiNodeSerializable
    final case class PutNextEvent(value: Int) extends Event

    final case class State(value: Int) extends STMultiNodeSerializable {
      def nextValue: Int = value + 1
    }

    def apply(
        system: ActorSystem[_],
    ): ReplicatedEntity[Command, ReplicationEnvelope[Command]] = {
      ReplicatedEntity(TypeKey)(entityContext =>
        Behaviors.setup { context =>
          context.setLoggerName(Entity.getClass)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(InitialValue),
            commandHandler = commandHandler(context, _, _),
            eventHandler = eventHandler,
          )
        },
      )
    }

    private def commandHandler(context: ActorContext[Command], state: State, command: Command): Effect[Event, State] =
      command match {
        case PutNext(value, replyTo) =>
          if (value < state.nextValue) {
            Effect.none.thenReply(replyTo) { state => StatusReply.Success(state.value) }
          } else if (value > state.nextValue) {
            Effect.none.thenReply(replyTo) { _ =>
              StatusReply.Error(s"expected ${state.nextValue}, but got $value.")
            }
          } else {
            assert(value == state.nextValue)
            context.log.info("Replicating PutNextEvent({}) in State({}).", value, state.value)
            Effect
              .replicate(PutNextEvent(value))
              .thenReply(replyTo) { newState =>
                StatusReply.Success(newState.value)
              }
          }
        case Get(replyTo) =>
          Effect.none.thenReply(replyTo)(_.value)
      }

    private def eventHandler(state: State, event: Event): State =
      event match {
        case PutNextEvent(value) =>
          assert(value == state.nextValue)
          state.copy(value)
      }

  }

}
