package lerna.akka.entityreplication

import akka.actor.{ Actor, ActorRef, Props }
import akka.cluster.ClusterEvent
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.testkit.{ TestActors, TestProbe }
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.jdk.CollectionConverters._

object ReplicationRegionInitializingSpecConfig extends MultiNodeConfig {
  val controller: RoleName = role("controller")
  val node1: RoleName      = role("node1")
  val node2a: RoleName     = role("node2a")
  val node2b: RoleName     = role("node2b")
  val node3: RoleName      = role("node3")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1  -> MemberIndex("member-1"),
    node2a -> MemberIndex("member-2"),
    node2b -> MemberIndex("member-2"),
    node3  -> MemberIndex("member-3"),
  )
  val multiRaftRoles = Set("member-1", "member-2", "member-3", "member-4", "member-5")

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString(s"""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      """))
      .withValue(
        "lerna.akka.entityreplication.raft.multi-raft-roles",
        ConfigValueFactory.fromIterable(multiRaftRoles.asJava),
      )
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node1).role}"]
  """))
  nodeConfig(node2a)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2a).role}"]
  """))
  nodeConfig(node2b)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2b).role}"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node3).role}"]
  """))

  // Enable the test transport feature for emulating network connectivity issues.
  testTransport(on = true)
}

object ReplicationRegionInitializingSpec {

  private object TestReplicationActor {

    def props: Props = TestActors.blackholeProps

    final case class Message(from: RoleName, value: String) extends STMultiNodeSerializable

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case message: Message => ("entity_1", message)
    }

    val extractShardId: ReplicationRegion.ExtractShardId = {
      case _: Message => "shard_1"
    }

  }

}

class ReplicationRegionInitializingSpecMultiJvmController extends ReplicationRegionInitializingSpec
class ReplicationRegionInitializingSpecMultiJvmNode1      extends ReplicationRegionInitializingSpec
class ReplicationRegionInitializingSpecMultiJvmNode2a     extends ReplicationRegionInitializingSpec
class ReplicationRegionInitializingSpecMultiJvmNode2b     extends ReplicationRegionInitializingSpec
class ReplicationRegionInitializingSpecMultiJvmNode3      extends ReplicationRegionInitializingSpec

class ReplicationRegionInitializingSpec
    extends MultiNodeSpec(ReplicationRegionInitializingSpecConfig)
    with STMultiNodeSpec {

  import ReplicationRegionInitializingSpec._
  import ReplicationRegionInitializingSpecConfig._

  override def initialParticipants: Int = roles.size

  val typeName: String          = "type-name_replication-region-initializing-spec"
  val raftActorProbe: TestProbe = TestProbe("RaftActorProbe")

  def getCurrentClusterState: ClusterEvent.CurrentClusterState = {
    val probe = TestProbe()
    cluster.sendCurrentClusterState(probe.ref)
    probe.expectMsgType[ClusterEvent.CurrentClusterState]
  }

  def createClusterEventProbe(): TestProbe = {
    val probe = TestProbe()
    cluster.subscribe(
      probe.ref,
      classOf[ClusterEvent.MemberUp],
      classOf[ClusterEvent.MemberRemoved],
      classOf[ClusterEvent.ReachableMember],
      classOf[ClusterEvent.UnreachableMember],
    )
    probe
  }

  def spawnReplicationRegion(): ActorRef = {
    system.actorOf(
      Props(
        new ReplicationRegion(
          typeName = typeName,
          _ => TestReplicationActor.props,
          ClusterReplicationSettings.create(system),
          TestReplicationActor.extractEntityId,
          TestReplicationActor.extractShardId,
          possibleShardIds = Set.empty,
          commitLogStore = system.deadLetters,
        ) {
          override def createRaftActorProps(): Props =
            Props(new Actor {
              override def receive: Receive = {
                case msg => raftActorProbe.ref forward msg
              }
            })
        },
      ),
    )
  }

  "ReplicationRegion" when {

    // Use for all nodes:
    var replicationRegion: ActorRef = null

    // Use for node1:
    var clusterEventProbe: TestProbe = null

    "the initializing state" should {

      "handle a CurrentClusterState." in {
        joinCluster(controller, node1)

        runOn(node1) {
          // Ensure the current cluster state contains node1.
          awaitAssert {
            clusterEventProbe = createClusterEventProbe()
            val currentClusterState = clusterEventProbe.expectMsgType[ClusterEvent.CurrentClusterState]
            currentClusterState.members.map(_.address) should contain(node(node1).address)
          }
        }

        runOn(node1) {
          replicationRegion = spawnReplicationRegion()
          // The ReplicationRegion of node1 will handle a CurrentClusterState in the background.
        }
      }

      "stash messages in the initializing-1." in {
        runOn(node1) {
          replicationRegion ! ReplicationRegion.Broadcast(
            RaftProtocol.Command(
              TestReplicationActor.Message(myself, "initializing-1"),
            ),
          )
        }
      }

      "handle a MemberUp event." in {
        joinCluster(node2a)

        runOn(node1) {
          val memberUpEvent = clusterEventProbe.fishForSpecificMessage[ClusterEvent.MemberUp]() {
            case memberUp: ClusterEvent.MemberUp => memberUp
          }
          memberUpEvent.member.address should be(node(node2a).address)
          // The ReplicationRegion of node1 will handle a MemberUp event in the background.
        }

        runOn(node2a) {
          replicationRegion = spawnReplicationRegion()
        }
      }

      "stash messages in the initializing-2." in {
        runOn(node1, node2a) {
          replicationRegion ! ReplicationRegion.Broadcast(
            RaftProtocol.Command(
              TestReplicationActor.Message(myself, "initializing-2"),
            ),
          )
        }
      }

      "handle an UnreachableMember event." in {
        isolate(node2a)

        runOn(node1) {
          val unreachableMemberEvent = clusterEventProbe.fishForSpecificMessage[ClusterEvent.UnreachableMember]() {
            case unreachableMember: ClusterEvent.UnreachableMember => unreachableMember
          }
          unreachableMemberEvent.member.address should be(node(node2a).address)
          // The ReplicationRegion of node1 will handle an UnreachableMember event in the background.
        }
      }

      "stash messages in the initializing-3." in {
        runOn(node1, node2a) {
          replicationRegion ! ReplicationRegion.Broadcast(
            RaftProtocol.Command(
              TestReplicationActor.Message(myself, "initializing-3"),
            ),
          )
        }
      }

      "handle a ReachableMember event." in {
        releaseIsolation(node2a)

        runOn(node1) {
          val reachableMemberEvent = clusterEventProbe.fishForSpecificMessage[ClusterEvent.ReachableMember]() {
            case reachableMember: ClusterEvent.ReachableMember => reachableMember
          }
          reachableMemberEvent.member.address should be(node(node2a).address)
          // The ReplicationRegion of node1 will handle a ReachableMember event in the background.
        }
      }

      "stash messages in the initializing-4." in {
        runOn(node1, node2a) {
          replicationRegion ! ReplicationRegion.Broadcast(
            RaftProtocol.Command(
              TestReplicationActor.Message(myself, "initializing-4"),
            ),
          )
        }
      }

      "handle a MemberRemoved event." in {
        leaveCluster(node2a)

        runOn(node1) {
          val memberRemovedEvent = clusterEventProbe.fishForSpecificMessage[ClusterEvent.MemberRemoved]() {
            case memberRemoved: ClusterEvent.MemberRemoved => memberRemoved
          }
          memberRemovedEvent.member.address should be(node(node2a).address)
          // The ReplicationRegion of node1 will handle a MemberRemoved event in the background.
        }
      }

      "stash messages in the initializing-5." in {
        runOn(node2b, node3) {
          replicationRegion = spawnReplicationRegion()
        }

        runOn(node1, node2b, node3) {
          replicationRegion ! ReplicationRegion.Broadcast(
            RaftProtocol.Command(
              TestReplicationActor.Message(myself, "initializing-5"),
            ),
          )
        }
      }

    }

    "the open state" should {

      "handle all messages stashed by the current cluster members" in {
        // The ReplicationRegion will be in the open state once the cluster has a quorum (3 nodes).
        joinCluster(node2b, node3)
        runOn(node1, node2b, node3) {
          awaitAssert {
            val memberAddresses = getCurrentClusterState.members.map(_.address)
            memberAddresses should contain(node(node1).address)
            memberAddresses shouldNot contain(node(node2a).address)
            memberAddresses should contain(node(node2b).address)
            memberAddresses should contain(node(node3).address)
          }
        }

        // The ReplicationRegion should handle stashed messages once it transits to the open state.
        // ReplicationRegions (of node1, node2b, and node3) don't handle messages that the ReplicationRegion of node2a
        // stashed.
        runOn(node1, node2b, node3) {
          val expectedHandledMessages = Set(
            TestReplicationActor.Message(node1, "initializing-1"),
            TestReplicationActor.Message(node1, "initializing-2"),
            TestReplicationActor.Message(node1, "initializing-3"),
            TestReplicationActor.Message(node1, "initializing-4"),
            TestReplicationActor.Message(node1, "initializing-5"),
            TestReplicationActor.Message(node2b, "initializing-5"),
            TestReplicationActor.Message(node3, "initializing-5"),
          ).map(RaftProtocol.Command)
          val handledMessages = raftActorProbe.receiveN(expectedHandledMessages.size)
          handledMessages should contain theSameElementsAs expectedHandledMessages
          raftActorProbe.expectNoMessage()
        }
      }

    }

  }

}
