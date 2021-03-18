package lerna.akka.entityreplication

import akka.actor.{ ActorRef, Props }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.RaftActorCompactionSpec.DummyReplicationActor
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

object RaftActorCompactionSpecConfig extends MultiNodeConfig {
  val controller: RoleName = role("controller")
  val node1: RoleName      = role("node1")
  val node2: RoleName      = role("node2")
  val node3: RoleName      = role("node3")

  testTransport(true)

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      
      lerna.akka.entityreplication.raft.multi-raft-roles = ["replica-group-1", "replica-group-2", "replica-group-3"]
      
      // triggers compaction each event replications
      lerna.akka.entityreplication.raft.compaction.log-size-threshold = 2
      lerna.akka.entityreplication.raft.compaction.preserve-log-size = 1
      lerna.akka.entityreplication.raft.compaction.log-size-check-interval = 0.1s
      """))
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString("""
    akka.cluster.roles = ["replica-group-1"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString("""
    akka.cluster.roles = ["replica-group-2"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString("""
    akka.cluster.roles = ["replica-group-3"]
  """))
}

class RaftActorCompactionSpecMultiJvmController extends RaftActorCompactionSpec
class RaftActorCompactionSpecMultiJvmNode1      extends RaftActorCompactionSpec
class RaftActorCompactionSpecMultiJvmNode2      extends RaftActorCompactionSpec
class RaftActorCompactionSpecMultiJvmNode3      extends RaftActorCompactionSpec

object RaftActorCompactionSpec {

  object DummyReplicationActor {

    def props(): Props = Props(new DummyReplicationActor())

    sealed trait Command extends STMultiNodeSerializable {
      def id: String
    }

    final case class Increment(id: String, amount: Int) extends Command
    final case class GetState(id: String)               extends Command

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }

    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }

    final case class Incremented(amount: Int) extends STMultiNodeSerializable

    final case class State(count: Int) extends STMultiNodeSerializable {
      def increment(amount: Int): State = copy(count = count + amount)
    }
  }

  import DummyReplicationActor._

  class DummyReplicationActor extends ReplicationActor[State] {

    private[this] var state: State = State(0)

    override def currentState: State = state

    override def receiveReplica: Receive = {
      case SnapshotOffer(snapshot: State) => state = snapshot
      case event: Incremented             => updateState(event)
    }

    override def receiveCommand: Receive = {
      case increment: Increment =>
        replicate(Incremented(increment.amount)) { event =>
          updateState(event)
          sender() ! state
        }
      case _: GetState =>
        sender() ! state
    }

    private[this] def updateState(event: Incremented): Unit = {
      state = state.increment(event.amount)
      println(s"state updated: $state")
    }
  }
}

class RaftActorCompactionSpec extends MultiNodeSpec(RaftActorCompactionSpecConfig) with STMultiNodeSpec {

  import RaftActorCompactionSpecConfig._

  override def initialParticipants: Int = roles.size

  "RaftActor" should {

    var clusterReplication: ActorRef = null

    "wait for all nodes to join the cluster" in {
      joinCluster(controller, node1, node2, node3)
    }

    "synchronize snapshot to recover a Follower even if the Follower could not receive logs by compaction in a Leader" in {
      val typeName = createUniqueTypeName()

      runOn(node1, node2, node3) {
        clusterReplication = createReplication(typeName)
      }

      val entityId = "test"
      def replicationActor(role: RoleName) = {
        system
          .actorSelection(
            s"${node(role).address}/system/sharding/raft-shard-$typeName-*/*/*/$entityId",
          ).resolveOne().await
      }

      runOn(node1) {
        // checks normality
        awaitAssert {
          clusterReplication ! DummyReplicationActor.GetState(entityId)
          expectMsgType[DummyReplicationActor.State](max = 5.seconds)
        }
        // updates state
        clusterReplication ! DummyReplicationActor.Increment(entityId, amount = 1)
        expectMsg(DummyReplicationActor.State(1))
      }
      enterBarrier("a command sent")

      runOn(node1, node2, node3) {
        awaitAssert {
          replicationActor(myself) ! DummyReplicationActor.GetState(entityId)
          expectMsg(DummyReplicationActor.State(1))
        }
      }
      enterBarrier("all ReplicationActor applied an event")

      isolate(node3, excludes = Set(controller))

      runOn(node1) {
        // checks normality
        awaitAssert {
          clusterReplication ! DummyReplicationActor.GetState(entityId)
          expectMsgType[DummyReplicationActor.State](max = 5.seconds)
        }
        // updates state
        clusterReplication ! DummyReplicationActor.Increment(entityId, amount = 1)
        clusterReplication ! DummyReplicationActor.Increment(entityId, amount = 1)
        clusterReplication ! DummyReplicationActor.Increment(entityId, amount = 1)
        receiveN(3)
      }
      enterBarrier("additional commands sent")

      runOn(node1, node2) {
        // ReplicationActor which has not been isolated applied all events
        awaitAssert {
          replicationActor(myself) ! DummyReplicationActor.GetState(entityId)
          expectMsg(DummyReplicationActor.State(4))
        }
      }
      runOn(node3) {
        // ReplicationActor which has been isolated did not apply any events
        awaitAssert {
          replicationActor(myself) ! DummyReplicationActor.GetState(entityId)
          expectMsg(DummyReplicationActor.State(1))
        }
      }
      enterBarrier("ReplicationActor which has not been isolated applied all events")

      releaseIsolation(node3)

      runOn(node1) {
        awaitAssert {
          // attempt to create entities on all nodes
          clusterReplication ! DummyReplicationActor.Increment(entityId, amount = 0)
          expectMsg(max = 3.seconds, DummyReplicationActor.State(4))

          import org.scalatest.Inspectors._
          // All ReplicationActor states will eventually be the same as any other after the isolation is resolved
          forAll(Set(node1, node2, node3)) { role =>
            replicationActor(role) ! DummyReplicationActor.GetState(entityId)
            expectMsg(max = 3.seconds, DummyReplicationActor.State(4))
          }
        }
      }
    }
  }

  def createReplication(typeName: String): ActorRef =
    planAutoKill {
      ClusterReplication(system).start(
        typeName = typeName,
        entityProps = DummyReplicationActor.props(),
        settings = ClusterReplicationSettings(system),
        extractEntityId = DummyReplicationActor.extractEntityId,
        extractShardId = DummyReplicationActor.extractShardId,
      )
    }

  private[this] val typeNameUniqueSeq              = new AtomicInteger(0)
  private[this] def createUniqueTypeName(): String = s"typeNam-${typeNameUniqueSeq.incrementAndGet()}"
}
