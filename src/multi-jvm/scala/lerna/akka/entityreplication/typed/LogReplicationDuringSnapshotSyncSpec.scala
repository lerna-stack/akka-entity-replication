package lerna.akka.entityreplication.typed

import akka.actor.RootActorPath
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.testkit.TestProbe
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.util.AtLeastOnceComplete
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Success, Try }

object LogReplicationDuringSnapshotSyncSpecConfig extends MultiNodeConfig {

  val controller: RoleName = role("controller")

  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")
  val node4: RoleName = role("node4")

  testTransport(true)

  private val testConfig: Config =
    ConfigFactory.parseString {
      """
      lerna.akka.entityreplication.raft {
      
        multi-raft-roles = ["member-1", "member-2", "member-3",]
        // By using a smaller value than this, the RaftActor gets a chance to become the leader.
        election-timeout = 999s
        // EntityReplication runs only one RaftActor group for the sake of simplicity.
        number-of-shards = 1
        raft-actor-auto-start.number-of-actors = 1
        
        sharding {
          // Sharding will be available as possible quick.
          retry-interval = 500ms
          waiting-for-state-timeout = 500ms
          updating-state-timeout = 500ms
          
          buffer-size = 50
        }
        compaction {
          // RaftActors will check compaction needs frequently.
          log-size-check-interval = 5000ms
          preserve-log-size = 1
        }
      }
      // This spec requires longer timeouts
      akka.actor.testkit.typed.filter-leeway = 30s
      akka.testconductor.barrier-timeout = 5m
      // This spec don't use the eventsourced feature
      lerna.akka.entityreplication.raft.eventsourced.commit-log-store.retry.attempts = 0
      """
    }

  commonConfig(
    debugConfig(false)
      .withFallback(testConfig)
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )

  nodeConfig(controller)(ConfigFactory.parseString {
    """
    // This node will not join cluster-replication
    akka.cluster.roles = []
    """
  })

  nodeConfig(node1)(ConfigFactory.parseString {
    """
    akka.cluster.roles = ["member-1"]
    // This node will become a leader
    lerna.akka.entityreplication.raft.heartbeat-interval = 50ms
    lerna.akka.entityreplication.raft.election-timeout = 750ms
    lerna.akka.entityreplication.raft.compaction.log-size-threshold = 3005
    """
  })

  nodeConfig(node2)(ConfigFactory.parseString {
    """
    akka.cluster.roles = ["member-2"]
    // This node will become a leader
    lerna.akka.entityreplication.raft.heartbeat-interval = 50ms
    lerna.akka.entityreplication.raft.election-timeout = 750ms
    // To disable compaction
    lerna.akka.entityreplication.raft.compaction.log-size-threshold = 999999
    """
  })

  nodeConfig(node3)(ConfigFactory.parseString {
    """
    akka.cluster.roles = ["member-3"]
    // To disable compaction
    lerna.akka.entityreplication.raft.compaction.log-size-threshold = 999999
    // We should delay the completion of the snapshot synchronization 
    lerna.akka.entityreplication.raft.snapshot-sync.snapshot-copying-parallelism = 1
    """
  })

  nodeConfig(node4)(ConfigFactory.parseString {
    """
    // Successor of node1
    akka.cluster.roles = ["member-1"]
    lerna.akka.entityreplication.raft.heartbeat-interval = 50ms
    lerna.akka.entityreplication.raft.election-timeout = 750ms
    """
  })
}

class LogReplicationDuringSnapshotSyncSpecMultiJvmController extends LogReplicationDuringSnapshotSyncSpec
class LogReplicationDuringSnapshotSyncSpecMultiJvmNode1      extends LogReplicationDuringSnapshotSyncSpec
class LogReplicationDuringSnapshotSyncSpecMultiJvmNode2      extends LogReplicationDuringSnapshotSyncSpec
class LogReplicationDuringSnapshotSyncSpecMultiJvmNode3      extends LogReplicationDuringSnapshotSyncSpec
class LogReplicationDuringSnapshotSyncSpecMultiJvmNode4      extends LogReplicationDuringSnapshotSyncSpec

class LogReplicationDuringSnapshotSyncSpec
    extends MultiNodeSpec(LogReplicationDuringSnapshotSyncSpecConfig)
    with STMultiNodeSpec {
  import LogReplicationDuringSnapshotSyncSpecConfig._
  import LogReplicationDuringSnapshotSyncSpec._

  private implicit val typedSystem: ActorSystem[_] = system.toTyped

  private val clusterReplication = ClusterReplication(typedSystem)

  // `.await` waits completion until QueryTimeout
  private val askTimeout: Timeout = Timeout(testConductor.Settings.QueryTimeout.duration - 100.millis)

  private val giveUpEasilyTimeout: Timeout = 1.seconds

  def setValue(id: String, value: Int)(implicit timeout: Timeout = askTimeout): Try[Int] = {
    import system.dispatcher
    val entityRef = clusterReplication.entityRefFor(Register.TypeKey, id)
    val reply     = AtLeastOnceComplete.askTo(entityRef, Register.Set(value, _), retryInterval = 400.millis)
    reply.transformWith(Future.successful).await.map(_.value)
  }

  def getValue(id: String)(implicit timeout: Timeout = askTimeout): Option[Int] = {
    val entityRef = clusterReplication.entityRefFor(Register.TypeKey, id)
    val reply     = AtLeastOnceComplete.askTo(entityRef, Register.Get(_), retryInterval = 400.millis)
    reply.await.value
  }

  def expectNewLeaderElected(f: => Unit): Unit = {
    LoggingTestKit.info("New leader was elected").expect(f)
  }

  private val resultCollector = TestProbe()

  private val resultCollectorSelection =
    system.actorSelection(RootActorPath(node(controller).address) / resultCollector.ref.path.elements)

  private var collectedResults: Seq[Try[Int]] = Seq.empty

  private def sendSetResultsToController(results: Seq[Try[Int]]): Unit = {
    runOn(myself) {
      resultCollectorSelection ! SetResults(results)
    }
  }

  private def receiveAndRecordResults(): Unit = {
    assert(myself == controller, s"This method should be call on ${controller}")
    // This operation should wait for another node sends the message
    val receivedResults =
      resultCollector.expectMsgType[SetResults](max = remainingOrDefault * 5)
    this.collectedResults ++= receivedResults.results
  }

  private def sendGotResultsToController(results: Seq[Int]): Unit = {
    runOn(myself) {
      resultCollectorSelection ! GotResults(results)
    }
  }

  private def receiveGotResults(): Seq[Int] = {
    assert(myself == controller, s"This method should be call on ${controller}")
    // This operation should wait for another node sends the message
    resultCollector.expectMsgType[GotResults](max = remainingOrDefault * 5).results
  }

  "LogReplicationDuringSnapshotSyncSpec" should {

    "elect node1 as a leader" in {
      newCluster(controller, node1, node2, node3)
      runOn(node1) {
        expectNewLeaderElected {
          clusterReplication.init(Register(typedSystem))
        }
      }
      runOn(node3) {
        clusterReplication.init(Register(typedSystem))
      }
      enterBarrier("Node1 is elected as a leader")
    }

    "join node2 to the cluster-replication" in {
      // for preventing node2 become a leader
      runOn(node2) {
        clusterReplication.init(Register(typedSystem))
      }
      enterBarrier("Node2 is joined the cluster")
    }

    "replicate log entries from the leader (node1)" in {
      runOn(node1) {
        // This replicated log creates a lots of snapshot to copy in snapshot synchronization
        val results =
          (1 to 3000).map { n =>
            val result = setValue(n.toString, n)
            result should be(Success(n))
            result
          }
        sendSetResultsToController(results)
      }
      runOn(controller) {
        receiveAndRecordResults()
      }
      enterBarrier("The leader replicated the log entries")
    }

    "compact the log on the leader (node1) after isolation for node3" in {
      isolate(node3)
      runOn(node1) {
        LoggingTestKit.info("compaction completed").expect {
          // This range should contain value of lerna.akka.entityreplication.raft.compaction.log-size-threshold
          val results =
            (3001 to 3010).map { n =>
              val result = setValue(n.toString, n)
              result should be(Success(n))
              result
            }
          sendSetResultsToController(results)
        }
      }
      runOn(controller) {
        receiveAndRecordResults()
      }
      enterBarrier("The leader compacted the log")
    }

    "send InstallSnapshot to the follower (node3) from the leader (node1), and elect node2 as a new leader" in {
      releaseIsolation(node3)
      runOn(node3) {
        LoggingTestKit.info("Snapshot synchronization started").expect {
          // this is triggered by releaseIsolation(node3)
        }
      }
      leaveClusterAsync(node1)
      runOn(node2) {
        // node2 become leader
        expectNewLeaderElected {
          // this is triggered by leaveClusterAsync(node1)
        }
      }
      enterBarrier("Snapshot synchronization started and the new leader was elected")
    }

    "append new log entries before snapshot synchronization completed and recover the former leader which has the 'member-1' role with node4" in {
      runOn(node2) {
        val results =
          (3011 to 3012).map { n =>
            // This operations can fail
            setValue(n.toString, n)(giveUpEasilyTimeout)
          }
        sendSetResultsToController(results)
      }
      leaveClusterAsync(node2)
      joinCluster(node4)
      runOn(controller) {
        receiveAndRecordResults()
      }
      enterBarrier("Log entries was appended and cluster formation was changed")
      runOn(node3) {
        LoggingTestKit.info("Snapshot synchronization completed").expect {
          // do nothing
        }
      }
      runOn(node4) {
        expectNewLeaderElected {
          clusterReplication.init(Register(typedSystem))
        }
      }
      enterBarrier("The former leader recovered")
    }

    "verify results" in {
      runOn(node3) {
        val results =
          (1 to 3012).flatMap { n =>
            // ignore None values
            getValue(n.toString)
          }
        sendGotResultsToController(results)
      }
      runOn(controller) {
        val currentState         = receiveGotResults().toSet
        val pastSucceededResults = this.collectedResults.filter(_.isSuccess).map(_.get).toSet
        // past-succeeded results and current state should be the same
        (pastSucceededResults union currentState) diff (pastSucceededResults intersect currentState) should be(empty)
      }
      enterBarrier("Verification completed")
    }

  }

}

object LogReplicationDuringSnapshotSyncSpec {

  final case class SetResults(results: Seq[Try[Int]]) extends STMultiNodeSerializable
  final case class GotResults(results: Seq[Int])      extends STMultiNodeSerializable

  object Register {
    val TypeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("register")

    sealed trait Command                                          extends STMultiNodeSerializable
    final case class Get(replyTo: ActorRef[GetReply])             extends Command
    final case class GetReply(value: Option[Int])                 extends STMultiNodeSerializable
    final case class Set(value: Int, replyTo: ActorRef[SetReply]) extends Command
    final case class SetReply(value: Int)                         extends STMultiNodeSerializable

    sealed trait Event                    extends STMultiNodeSerializable
    final case class SetEvent(value: Int) extends Event

    /**
      * We should set large data to delay snapshot synchronization
      * Long (8byte) * 16 * 1024 = 128 KB
      */
    private val dummyLargeData: Seq[Long] = Seq.fill(16 * 1024)(Long.MaxValue)

    final case class State(value: Option[Int], dummyLargeData: Seq[Long]) extends STMultiNodeSerializable

    def apply(system: ActorSystem[_]): ReplicatedEntity[Command, ReplicationEnvelope[Command]] = {
      val settings = ClusterReplicationSettings(system)
      ReplicatedEntity(TypeKey)(entityContext =>
        Behaviors.setup { context =>
          context.setLoggerName(Register.getClass)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext,
            emptyState = State(value = None, dummyLargeData),
            commandHandler,
            eventHandler,
          )
        },
      ).withSettings(settings)
    }

    def commandHandler(state: State, command: Command): Effect[Event, State] =
      command match {
        case Set(value, replyTo) =>
          state.value match {
            case Some(`value`) =>
              Effect
                .none[Event, State]
                .thenPassivate()
                .thenReply(replyTo)(_ => SetReply(value))
            case _ =>
              Effect
                .replicate(SetEvent(value))
                .thenPassivate()
                .thenReply(replyTo) {
                  case State(Some(value), _) => SetReply(value)
                  case _                     => throw new IllegalStateException()
                }
          }
        case Get(replyTo) =>
          Effect.none.thenReply(replyTo)(state => GetReply(state.value))
      }

    def eventHandler(state: State, event: Event): State =
      event match {
        case SetEvent(value) => state.copy(value = Option(value))
      }
  }

}
