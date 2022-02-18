package lerna.akka.entityreplication.typed

import akka.actor
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.{ actor => classic }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.remote.testconductor._
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.util.AtLeastOnceComplete
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object MultiSnapshotSyncSpecConfig extends MultiNodeConfig {
  val node1: RoleName  = role("node1")
  val node2: RoleName  = role("node2")
  val node3: RoleName  = role("node3")
  val node4: RoleName  = role("node4")
  val node5: RoleName  = role("node5")
  val node6: RoleName  = role("node6")
  val node7: RoleName  = role("node7")
  val node8: RoleName  = role("node8")
  val node9: RoleName  = role("node9")
  val node10: RoleName = role("node10")
  val node11: RoleName = role("node11")

  private val testConfig: Config =
    ConfigFactory
      .parseString(s"""
          |lerna.akka.entityreplication.raft.multi-raft-roles = [
          |  "member-1", "member-2", "member-3"
          |]
          |lerna.akka.entityreplication {
          |  // RaftActors will recover entities as possible quick.
          |  recovery-entity-timeout = 1s
          |  raft {
          |    // Using this default value, RaftActors have a chance to become the leader.
          |    // By using a larger value, RaftActors don't have such a chance.
          |    election-timeout = 1s
          |    // EntityReplication runs only one RaftActor group for the sake of simplicity.
          |    number-of-shards = 1
          |    raft-actor-auto-start {
          |      number-of-actors = 1
          |    }
          |    sharding {
          |      // Sharding will be available as possible quick.
          |      retry-interval = 1s
          |      waiting-for-state-timeout = 1s
          |      updating-state-timeout = 1s
          |    }
          |    // RaftActors will check compaction needs frequently.
          |    compaction {
          |      log-size-check-interval = 3s
          |      log-size-threshold = 10
          |      preserve-log-size = 1
          |    }
          |  }
          |}
          |""".stripMargin)

  commonConfig(
    debugConfig(false)
      .withFallback(testConfig)
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )

  nodeConfig(node2)(
    ConfigFactory
      .parseString("""akka.cluster.roles = ["member-1"]"""),
  )
  nodeConfig(node3)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-2"]
          |# By using a larger value, this node won't compact log entries.
          |# A node can compact log entries less frequently than others.
          |lerna.akka.entityreplication.raft.compaction.log-size-threshold = 100
          |""".stripMargin),
  )

  nodeConfig(node4)(
    ConfigFactory
      .parseString("""akka.cluster.roles = ["member-1"]"""),
  )
  nodeConfig(node5)(
    ConfigFactory
      .parseString("""akka.cluster.roles = ["member-3"]"""),
  )

  nodeConfig(node6)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-1"]
          |lerna.akka.entityreplication.raft.election-timeout = 100s
          |""".stripMargin),
  )
  nodeConfig(node7)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-2"]
          |lerna.akka.entityreplication.raft.election-timeout = 100s
          |""".stripMargin),
  )
  nodeConfig(node8)(
    ConfigFactory
      .parseString("""akka.cluster.roles = ["member-3"]"""),
  )

  nodeConfig(node9)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-1"]
          |lerna.akka.entityreplication.raft.election-timeout = 100s
          |""".stripMargin),
  )
  nodeConfig(node10)(
    ConfigFactory
      .parseString("""akka.cluster.roles = ["member-2"]"""),
  )
  nodeConfig(node11)(
    ConfigFactory
      .parseString("""
          |akka.cluster.roles = ["member-3"]
          |lerna.akka.entityreplication.raft.election-timeout = 100s
          |""".stripMargin),
  )

}

final class MultiSnapshotSyncSpecMultiJvmNode1  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode2  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode3  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode4  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode5  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode6  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode7  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode8  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode9  extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode10 extends MultiSnapshotSyncSpec
final class MultiSnapshotSyncSpecMultiJvmNode11 extends MultiSnapshotSyncSpec

class MultiSnapshotSyncSpec extends MultiNodeSpec(MultiSnapshotSyncSpecConfig) with STMultiNodeSpec {

  import MultiSnapshotSyncSpec._
  import MultiSnapshotSyncSpecConfig._

  /** ClusterReplication should be ready to handle requests within this timeout */
  private val initializationTimeout: FiniteDuration = 10.seconds

  /** RaftActor should compact it's log within this timeout */
  private val compactionTimeout: FiniteDuration = 10.seconds

  /** None if this node doesn't shut it down. */
  private var maybeNewSystem: Option[classic.ActorSystem] = None
  private var runningNodes: Seq[RoleName]                 = roles

  private implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
  private val clusterReplication: ClusterReplication     = ClusterReplication(typedSystem)

  override def initialParticipants: Int = 2

  // Disable `planAutoKill`, `beforeEach`, `afterEach`
  // since the node will shut the `system` down in this test.

  override def planAutoKill(ref: actor.ActorRef): actor.ActorRef = {
    throw new IllegalStateException("planAutoKill is not supported in this test.")
  }

  override def beforeEach(): Unit = {
    // By overriding this method, do nothing before each test.
  }

  override def afterEach(): Unit = {
    // By overriding this method, do nothing after each test.
  }

  override def afterAll(): Unit = {
    maybeNewSystem match {
      case Some(sys) =>
        shutdown(sys, duration = shutdownTimeout)
        afterTermination()
      case None =>
        multiNodeSpecAfterAll()
    }
  }

  /** Shuts the given node down.
    *
    * Some methods (such as [[leaveCluster]], [[joinCluster]]) will call [[enterBarrier]],
    * the node, that shuts itself down, have to call [[startNewSystem]] after it shuts down.
    *
    * The first node have to wait for the new [[TestConductor]] with the new [[classic.ActorSystem]] to be available,
    * since the first node cannot notify the new conductor a new barrier until the new conductor is registered.
    */
  private def shutdownNode(nodes: RoleName*): Unit = {
    assert(!nodes.contains(roles.head), "Cannot shutdown the first node.")
    val notRunningNodes = nodes.filterNot(runningNodes.contains(_))
    assert(notRunningNodes.isEmpty, s"Cannot shutdown the same node twice: $notRunningNodes.")
    nodes.foreach { node =>
      runningNodes = runningNodes.filterNot(_ == node)
      leaveCluster(node)
      runOn(roles.head) {
        testConductor.shutdown(node).await
      }
      runOn(node) {
        system.whenTerminated.await
        maybeNewSystem = Option(startNewSystem())
      }
      runOn(roles.head) {
        awaitCond {
          testConductor.getNodes.await.exists(_ == node)
        }
      }
    }
  }

  /** Executes the given block of code only on other than the given nodes */
  private def runOnOtherThan(nodes: RoleName*)(thunk: => Unit): Unit = {
    val otherNodes = roles.filterNot(nodes.contains(_))
    runOn(otherNodes: _*)(thunk)
  }

  /** Verifies compaction completed by inspecting logging
    *
    * The compaction should completes in the given block of code.
    */
  private def expectCompactionCompleted[T](code: => T): T = {
    LoggingTestKit.info("compaction completed").expect(code)
  }

  /** Waits for compaction completed by sleeping the current thread.
    *
    * The compaction will complete automatically and asynchronously.
    * Use [[expectCompactionCompleted]] if this test verifies the compaction completed.
    */
  private def waitForCompactionCompleted(): Unit = {
    Thread.sleep(compactionTimeout.toMillis)
  }

  private def setValue(id: String, value: Int)(implicit timeout: Timeout): Int = {
    val entityRef = clusterReplication.entityRefFor(Register.TypeKey, id)
    val reply     = AtLeastOnceComplete.askTo(entityRef, Register.Set(value, _), retryInterval = 200.millis)
    reply.await.value
  }

  private def getValue(id: String)(implicit timeout: Timeout): Int = {
    val entityRef = clusterReplication.entityRefFor(Register.TypeKey, id)
    val reply     = AtLeastOnceComplete.askTo(entityRef, Register.Get(_), retryInterval = 200.millis)
    reply.await.value
  }

  "A new cluster has nodes: [2,3,_]" in {
    newCluster(node2, node3)
    runOn(node2, node3) {
      clusterReplication.init(Register(typedSystem))
    }
    enterBarrier("The cluster has nodes: [2,3].")
  }

  "The leader replicates some log entries and then it compacts the entries" in {
    runOn(node2) {
      expectCompactionCompleted {
        setValue("0", 0)(initializationTimeout) shouldBe 0
        (1 to 10).foreach { n =>
          setValue(n.toString, n) shouldBe n
        }
        waitForCompactionCompleted()
      }
    }
    enterBarrier("Nodes([2,3]) replicated log entries (entity ids = 1 ~ 10) and node2 compacts its log entries.")
  }

  "The cluster has nodes: [4,_,5]" in {
    shutdownNode(node3, node2)
    newCluster(node4, node5)
    runOn(node4, node5) {
      clusterReplication.init(Register(typedSystem))
    }
    enterBarrier("The cluster has nodes: [4,_,5].")
  }

  "The leader (which is on node4) replicates some log entries. Nodes ([4,5]) compacts their log entries" in {
    object BarrierNames {
      val ExpectingCompactionCompleted = "Expecting compaction completed"
      val ReplicatedLogEntries         = "Replicated log entries"
    }
    runOn(node4) {
      expectCompactionCompleted {
        enterBarrier(BarrierNames.ExpectingCompactionCompleted)
        setValue("0", 0)(initializationTimeout) shouldBe 0
        (11 to 20).foreach { n =>
          setValue(n.toString, n) shouldBe n
        }
        enterBarrier(BarrierNames.ReplicatedLogEntries)
        waitForCompactionCompleted()
      }
    }
    runOn(node5) {
      expectCompactionCompleted {
        enterBarrier(BarrierNames.ExpectingCompactionCompleted)
        enterBarrier(BarrierNames.ReplicatedLogEntries)
        waitForCompactionCompleted()
      }
    }
    runOnOtherThan(node4, node5) {
      enterBarrier(BarrierNames.ExpectingCompactionCompleted)
      enterBarrier(BarrierNames.ReplicatedLogEntries)
    }
    enterBarrier("Nodes([4,5]) replicated log entries (entity ids = 11 ~ 20) and compacted their log entries.")
  }

  "The cluster has nodes: [6,7,8]" in {
    shutdownNode(node5, node4)
    newCluster(node6, node7, node8)
    runOn(node6, node7, node8) {
      clusterReplication.init(Register(typedSystem))
    }
    enterBarrier("The cluster has nodes: [6,7,8].")
  }

  "The leader (which is on node8) replicates some log entries" in {
    object BarrierNames {
      val ExpectingCompactionCompleted = "Expecting compaction completed"
      val ReplicatedLogEntries         = "Replicated log entries"
    }
    runOn(node6) {
      expectCompactionCompleted {
        enterBarrier(BarrierNames.ExpectingCompactionCompleted)
        setValue("0", 0)(initializationTimeout) shouldBe 0
        (21 to 30).foreach { n =>
          setValue(n.toString, n) shouldBe n
        }
        enterBarrier(BarrierNames.ReplicatedLogEntries)
        waitForCompactionCompleted()
      }
    }
    runOn(node7, node8) {
      expectCompactionCompleted {
        enterBarrier(BarrierNames.ExpectingCompactionCompleted)
        enterBarrier(BarrierNames.ReplicatedLogEntries)
        waitForCompactionCompleted()
      }
    }
    runOnOtherThan(node6, node7, node8) {
      enterBarrier(BarrierNames.ExpectingCompactionCompleted)
      enterBarrier(BarrierNames.ReplicatedLogEntries)
    }
    enterBarrier("Replicated log entries (entity ids = 21 ~ 30).")
  }

  "The new cluster has nodes: [9,10,11]" in {
    shutdownNode(node8, node7, node6)
    newCluster(node9, node10, node11)
    runOn(node9, node10, node11) {
      clusterReplication.init(Register(typedSystem))
    }
    enterBarrier("The new cluster has nodes: [9,10,11].")
  }

  "The leader (which is on node10) should have consistent state" in {
    runOn(node9) { // Can choose any of nodes [9,10,11]
      getValue("0")(initializationTimeout) shouldBe 0
      (1 to 30).foreach { n =>
        getValue(n.toString) shouldBe n
      }
    }
    enterBarrier("The leader has consistent state.")
  }

}

object MultiSnapshotSyncSpec {

  /** Holds a single integer value */
  object Register {
    val TypeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey(s"register")
    val InitialValue                              = 0

    sealed trait Command                                          extends STMultiNodeSerializable
    final case class Get(replyTo: ActorRef[GetReply])             extends Command
    final case class GetReply(value: Int)                         extends STMultiNodeSerializable
    final case class Set(value: Int, replyTo: ActorRef[SetReply]) extends Command
    final case class SetReply(value: Int)                         extends STMultiNodeSerializable

    sealed trait Event                    extends STMultiNodeSerializable
    final case class SetEvent(value: Int) extends Event

    final case class State(value: Int) extends STMultiNodeSerializable

    def apply(
        system: ActorSystem[_],
    ): ReplicatedEntity[Command, ReplicationEnvelope[Command]] = {
      val settings = ClusterReplicationSettings(system)
      ReplicatedEntity(TypeKey)(entityContext =>
        Behaviors.setup { context =>
          context.setLoggerName(Register.getClass)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(InitialValue),
            commandHandler = commandHandler,
            eventHandler = eventHandler,
          )
        },
      ).withSettings(settings)
    }

    // NOTE: Command Handler should be idempotent.
    def commandHandler(state: State, command: Command): Effect[Event, State] =
      command match {
        case Set(value, replyTo) =>
          if (state.value == value) {
            Effect.none.thenReply(replyTo) { state: State => SetReply(state.value) }
          } else {
            Effect
              .replicate(SetEvent(value))
              .thenReply(replyTo)(newState => SetReply(newState.value))
          }
        case Get(replyTo) =>
          Effect.none.thenReply(replyTo) { state: State => GetReply(state.value) }
      }

    def eventHandler(state: State, event: Event): State =
      event match {
        case SetEvent(value) =>
          state.copy(value)
      }

  }

}
