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

    sealed trait Command {
      def id: String
    }

    final case class Cmd(id: String)      extends Command
    final case class GetState(id: String) extends Command

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }

    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }

    final case object ReceivedEvent

    final case class State(count: Int) {
      def increment: State = copy(count = count + 1)
    }
  }

  import DummyReplicationActor._

  class DummyReplicationActor extends ReplicationActor[State] {

    private[this] var state: State = State(0)

    override def currentState: State = state

    override def receiveReplica: Receive = {
      case SnapshotOffer(snapshot: State) => state = snapshot
      case ReceivedEvent                  => updateState()
    }

    override def receiveCommand: Receive = {
      case _: Cmd =>
        replicate(ReceivedEvent) { _ =>
          updateState()
          sender() ! state
        }
      case _: GetState =>
        sender() ! state
    }

    private[this] def updateState(): Unit = {
      state = state.increment
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
      def replicationActor =
        system.actorSelection(s"/system/sharding/raft-shard-$typeName-*/*/*/$entityId").resolveOne().await

      runOn(node1) {
        // checks normality
        awaitAssert {
          clusterReplication ! DummyReplicationActor.GetState(entityId)
          expectMsgType[DummyReplicationActor.State](max = 5.seconds)
        }
        // updates state
        clusterReplication ! DummyReplicationActor.Cmd(entityId)
        expectMsg(DummyReplicationActor.State(1))
      }
      enterBarrier("a command sent")

      runOn(node1, node2, node3) {
        awaitAssert {
          replicationActor ! DummyReplicationActor.GetState(entityId)
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
        clusterReplication ! DummyReplicationActor.Cmd(entityId)
        clusterReplication ! DummyReplicationActor.Cmd(entityId)
        clusterReplication ! DummyReplicationActor.Cmd(entityId)
        receiveN(3)
      }
      enterBarrier("additional commands sent")

      runOn(node1, node2) {
        // ReplicationActor which has not been isolated applied all events
        awaitAssert {
          replicationActor ! DummyReplicationActor.GetState(entityId)
          expectMsg(DummyReplicationActor.State(4))
        }
      }
      runOn(node3) {
        // ReplicationActor which has been isolated did not apply any events
        awaitAssert {
          replicationActor ! DummyReplicationActor.GetState(entityId)
          expectMsg(DummyReplicationActor.State(1))
        }
      }
      enterBarrier("ReplicationActor which has not been isolated applied all events")

      releaseIsolation(node3)

      runOn(node1) {
        // ensures that entities exist on all nodes
        clusterReplication ! DummyReplicationActor.Cmd(entityId)
        expectMsg(DummyReplicationActor.State(5))
      }
      enterBarrier("a command sent")

      runOn(node1, node2, node3) {
        // All ReplicationActor states will eventually be the same as any other after the isolation is resolved
        awaitAssert {
          replicationActor ! DummyReplicationActor.GetState(entityId)
          expectMsg(DummyReplicationActor.State(5))
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
