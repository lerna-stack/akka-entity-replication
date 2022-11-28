package lerna.akka.entityreplication

import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

import scala.annotation.nowarn

object ConsistencyTestBase {

  object ConsistencyTestBaseConfig extends MultiNodeConfig {
    val node1: RoleName = role("node1")
    val node2: RoleName = role("node2")
    val node3: RoleName = role("node3")
    val node4: RoleName = role("node4")
    val node5: RoleName = role("node5")

    commonConfig(
      debugConfig(false).withFallback(
        ConfigFactory
          .parseString("""
             akka.actor.provider = cluster
              akka.test.single-expect-default = 15s
             lerna.akka.entityreplication.raft.multi-raft-roles = ["member-1", "member-2", "member-3"]
           """).withFallback(
            ConfigFactory.parseResources("multi-jvm-testing.conf"),
          ),
      ),
    )

    nodeConfig(node1)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-1"]
  """))
    nodeConfig(node2)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-2"]
  """))
    nodeConfig(node3)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-3"]
  """))
    nodeConfig(node4)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-1"]
  """))
    nodeConfig(node5)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-2"]
  """))
  }

  object ConsistencyTestReplicationActor {

    trait Command extends STMultiNodeSerializable {
      def id: String
      def requestId: String
    }
    trait Event extends STMultiNodeSerializable
    trait Response extends STMultiNodeSerializable {
      def requestId: String
    }

    case object Received extends Event

    case class CountUp(id: String, requestId: String)   extends Command
    case class GetStatus(id: String, requestId: String) extends Command

    case class Complete(requestId: String)           extends Response
    case class Status(count: Int, requestId: String) extends Response

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }

    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }
  }

  @nowarn("msg=Use typed.ReplicatedEntityBehavior instead")
  class ConsistencyTestReplicationActor() extends ReplicationActor[Int] {

    import ConsistencyTestReplicationActor._

    var count: Int = 0

    override def preStart(): Unit = {
      println(s"=== ${context.self.path} started ===")
    }

    override def receiveReplica: Receive = {
      case Received => updateState()
    }

    override def receiveCommand: Receive = {
      case cmd: CountUp =>
        replicate(Received) { _ =>
          updateState()
          sender() ! Complete(cmd.requestId)
        }
      case cmd: GetStatus =>
        ensureConsistency {
          sender() ! Status(count, cmd.requestId)
        }
    }

    def updateState(): Unit = {
      count += 1
    }

    override def currentState: Int = count
  }
}
