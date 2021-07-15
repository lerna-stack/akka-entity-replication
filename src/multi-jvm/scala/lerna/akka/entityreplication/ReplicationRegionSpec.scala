package lerna.akka.entityreplication

import akka.Done

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ Actor, ActorRef, ActorSelection, DiagnosticActorLogging, Props, RootActorPath, Terminated }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.testkit.TestProbe
import akka.util.Timeout
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.ReplicationRegionSpec.DummyReplicationActor.CheckRouting
import lerna.akka.entityreplication.raft.RaftProtocol.Command
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object ReplicationRegionSpec {

  case class ReceivedMessages(messages: Seq[CheckRouting], node: RoleName, memberIndex: MemberIndex)
      extends STMultiNodeSerializable

  object DummyReplicationActor {

    def props(probe: TestProbe) = Props(new DummyReplicationActor(probe))

    sealed trait Command extends STMultiNodeSerializable {
      def id: String
    }

    case class Cmd(id: String)          extends Command
    case class CheckRouting(id: String) extends Command

    case class GetStatus(id: String)                        extends Command
    case class GetStatusWithEnsuringConsistency(id: String) extends Command
    case class Status(count: Int)                           extends STMultiNodeSerializable

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }

    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }
  }

  @nowarn // for deprecated ReplicationActor
  class DummyReplicationActor(probe: TestProbe) extends DiagnosticActorLogging with ReplicationActor[Int] {

    import DummyReplicationActor._

    var count: Int = 0

    override def preStart(): Unit = {
      println(s"=== ${context.self.path} started ===")
    }

    override def receiveReplica: Receive = {
      case SnapshotOffer(snapshot: Int) => count = snapshot
      case "received"                   => updateState()
    }

    override def receiveCommand: Receive = {
      case _: Cmd =>
        replicate("received") { _ =>
          updateState()
        }
      case msg: CheckRouting =>
        probe.ref forward msg
      case GetStatus(_) =>
        // DummyReplicationActor の状態を直接確認する用
        sender() ! Status(count)
      case GetStatusWithEnsuringConsistency(_) =>
        log.info("receive: GetStatusWithEnsuringConsistency: {}", count)
        // ReplicationRegion 経由で状態を確認する用
        ensureConsistency {
          sender() ! Status(count)
        }
    }

    def updateState(): Unit = {
      log.info("updateState")
      count += 1
    }

    override def currentState: Int = count
  }
}

object ReplicationRegionSpecConfig extends MultiNodeConfig {
  val controller: RoleName = role("controller")
  val node1: RoleName      = role("node1")
  val node2: RoleName      = role("node2")
  val node3: RoleName      = role("node3")
  val node4: RoleName      = role("node4")
  val node5: RoleName      = role("node5")
  val node6: RoleName      = role("node6")
  val node7: RoleName      = role("node7")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-2"),
    node3 -> MemberIndex("member-3"),
    node4 -> MemberIndex("member-1"),
    node5 -> MemberIndex("member-2"),
    node6 -> MemberIndex("member-3"),
    node7 -> MemberIndex("member-1"),
  )

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString(s"""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      
      lerna.akka.entityreplication.raft.compaction.log-size-threshold = 2
      lerna.akka.entityreplication.raft.compaction.preserve-log-size = 1
      lerna.akka.entityreplication.raft.compaction.log-size-check-interval = 0.1s
      """))
      .withValue(
        "lerna.akka.entityreplication.raft.multi-raft-roles",
        ConfigValueFactory.fromIterable(memberIndexes.values.map(_.role).toSet.asJava),
      )
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node1).role}"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2).role}"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node3).role}"]
  """))
  nodeConfig(node4)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node4).role}"]
  """))
  nodeConfig(node5)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node5).role}"]
  """))
  nodeConfig(node6)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node6).role}"]
  """))
  nodeConfig(node7)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node7).role}"]
  """))
}

class ReplicationRegionSpecMultiJvmController extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode1      extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode2      extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode3      extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode4      extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode5      extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode6      extends ReplicationRegionSpec
class ReplicationRegionSpecMultiJvmNode7      extends ReplicationRegionSpec

class ReplicationRegionSpec extends MultiNodeSpec(ReplicationRegionSpecConfig) with STMultiNodeSpec {

  import ReplicationRegionSpec._
  import DummyReplicationActor._
  import ReplicationRegionSpecConfig._

  override def initialParticipants: Int = roles.size

  private[this] val controllerTestActor: ActorSelection =
    system.actorSelection(RootActorPath(node(controller).address) / testActor.path.elements)

  "ReplicationRegion" should {

    var clusterReplication: ActorRef = null

    val entityProbe = TestProbe("entity")

    val raftActorProbe = TestProbe("RaftActor")

    @nowarn // for deprecated ClusterReplication(system).start
    def createReplication(typeName: String): ActorRef =
      planAutoKill {
        ClusterReplication(system).start(
          typeName = typeName,
          entityProps = DummyReplicationActor.props(entityProbe),
          settings = ClusterReplicationSettings(system),
          extractEntityId = DummyReplicationActor.extractEntityId,
          extractShardId = DummyReplicationActor.extractShardId,
        )
      }

    def createReplicationWith(typeName: String, raftActorProbe: TestProbe): ActorRef =
      planAutoKill {
        system.actorOf(
          Props(
            new ReplicationRegion(
              typeName = typeName,
              _ => DummyReplicationActor.props(entityProbe),
              ClusterReplicationSettings(system),
              DummyReplicationActor.extractEntityId,
              DummyReplicationActor.extractShardId,
              maybeCommitLogStore = None,
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

    "wait for all nodes to enter a barrier" in {
      joinCluster(controller, node1, node2, node3)
    }

    "コマンドを受け取ったときに子の Entity が存在しない場合、対象の Entity を作成" in {
      val typeName = createSeqTypeName()

      runOn(node1, node2, node3) {
        clusterReplication = createReplication(typeName)
      }
      enterBarrier("ReplicationRegion created")

      val entityId = "test"
      runOn(node3) {
        clusterReplication ! Cmd(entityId)
      }
      enterBarrier("Some command sent")

      runOn(node1, node2, node3) {
        // Entity が各ノードで生成されていることを確認
        implicit val timeout: Timeout = Timeout(1.second)

        val role                 = memberIndexes(myself)
        val clusterShard         = "*" // hash of raftShardId
        val raftShardId          = "*" // hash of entityId
        val replicationActorPath = s"/system/sharding/raft-shard-$typeName-$role/$clusterShard/$raftShardId/$entityId"
        awaitAssert(system.actorSelection(replicationActorPath).resolveOne().await, max = 10.seconds)
      }
    }

    "子の Entity にコマンドを転送する" in {
      val typeName = createSeqTypeName()

      val entityId = createSeqEntityId()
      runOn(node1, node2, node3) {
        clusterReplication = createReplication(typeName)
      }
      enterBarrier("ReplicationRegion created")
      runOn(node3) {
        clusterReplication ! Cmd(entityId)
        clusterReplication ! GetStatusWithEnsuringConsistency(entityId)
        // コマンドにより更新された状態が取得できていることから
        // 正しくコマンドが転送できているとみなす
        expectMsgType[Status].count should be(1)
      }
    }

    "各 ReplicationActor の状態が同期する" in {
      val typeName = createSeqTypeName()
      val entityId = createSeqEntityId()
      runOn(node1, node2, node3) {
        clusterReplication = createReplication(typeName)
      }
      enterBarrier("ReplicationRegion created")
      runOn(node3) {
        clusterReplication ! Cmd(entityId)
        clusterReplication ! Cmd(entityId)
        clusterReplication ! Cmd(entityId)
        clusterReplication ! GetStatusWithEnsuringConsistency(entityId)
        expectMsg(Status(3))
      }
      enterBarrier("sent command")
      runOn(node1, node2, node3) {
        // 各ノードの ReplicationActor の状態が更新されている

        val role                 = memberIndexes(myself)
        val clusterShard         = "*" // hash of raftShardId
        val raftShardId          = "*" // hash of entityId
        val replicationActorPath = s"/system/sharding/raft-shard-$typeName-$role/$clusterShard/$raftShardId/$entityId"
        awaitAssert {
          system.actorSelection(replicationActorPath) ! GetStatus(entityId)
          expectMsg(Status(3))
        }
      }
    }

    "一度 Region を停止しても各 ReplicationActor の状態が復元する" in {
      val typeName = createSeqTypeName()
      val entityId = createSeqEntityId()
      runOn(node1, node2, node3) {
        clusterReplication = createReplication(typeName)
      }
      enterBarrier("ReplicationRegion created")
      runOn(node3) {
        clusterReplication ! Cmd(entityId)
        clusterReplication ! Cmd(entityId)
        clusterReplication ! Cmd(entityId)
        clusterReplication ! GetStatusWithEnsuringConsistency(entityId)
        expectMsg(Status(3))
        // wait for the compaction to finish
        akka.pattern.after(3.second)(Future.successful(Done)).await
      }
      enterBarrier("status replicated")
      runOn(node1, node2, node3) {
        val supervisor           = clusterReplication
        val role                 = memberIndexes(myself)
        val clusterShard         = "*" // hash of raftShardId
        val raftShardId          = "*" // hash of entityId
        val replicationActorPath = s"/system/sharding/raft-shard-$typeName-$role/$clusterShard/$raftShardId/$entityId"
        val replicationActor     = system.actorSelection(replicationActorPath).resolveOne().await
        watch(supervisor)
        watch(replicationActor)
        system.stop(supervisor)
        receiveWhile[Terminated](messages = 2) {
          case t @ Terminated(`supervisor`)       => t
          case t @ Terminated(`replicationActor`) => t
        }
      }
      enterBarrier("terminated all members")

      joinCluster(node4, node5, node6)
      leaveCluster(node1, node2, node3)

      runOn(node4, node5, node6) {
        clusterReplication = createReplication(typeName)
        clusterReplication ! GetStatusWithEnsuringConsistency(entityId)
        expectMsg(Status(3))
      }
    }

    "Broadcast" when {

      "全 MemberIndex に配信される" in {
        val typeName = createSeqTypeName()

        runOn(node4, node5, node6) {
          clusterReplication = createReplicationWith(typeName, raftActorProbe)
        }
        enterBarrier("ReplicationRegion created")

        val entityId = createSeqEntityId()

        val message = Command(CheckRouting(entityId))

        runOn(node4) {
          clusterReplication ! ReplicationRegion.Broadcast(message)
        }
        enterBarrier("message sent")

        runOn(node4, node5, node6) {
          raftActorProbe.expectMsg(message)
        }
      }
    }

    "BroadcastWithoutSelf" when {

      "自分を除く全 MemberIndex に配信される" in {
        val typeName = createSeqTypeName()

        runOn(node4, node5, node6) {
          clusterReplication = createReplicationWith(typeName, raftActorProbe)
        }
        enterBarrier("ReplicationRegion created")

        val entityId = createSeqEntityId()

        val message = Command(CheckRouting(entityId))

        runOn(node4) {
          clusterReplication ! ReplicationRegion.BroadcastWithoutSelf(message)
        }
        enterBarrier("message sent")

        runOn(node4) {
          expectNoMessage(max = 1.second)
        }
        runOn(node5, node6) {
          raftActorProbe.expectMsg(message)
        }
      }
    }

    "DeliverTo" when {

      "特定の MemberIndex に配信される" in {
        val typeName = createSeqTypeName()

        runOn(node4, node5, node6) {
          clusterReplication = createReplicationWith(typeName, raftActorProbe)
        }
        enterBarrier("ReplicationRegion created")

        val entityId = createSeqEntityId()

        val message = Command(CheckRouting(entityId))

        runOn(node4) {
          clusterReplication ! ReplicationRegion.DeliverTo(memberIndexes(node6), message)
        }
        enterBarrier("message sent")

        runOn(node6) {
          raftActorProbe.expectMsg(message)
        }
        runOn(node4, node5) {
          expectNoMessage(max = 1.second)
        }
      }
    }

    "DeliverSomewhere" when {

      "いずれかの MemberIndex に配信される" in {
        val typeName = createSeqTypeName()
        val entityId = createSeqEntityId()

        val message = CheckRouting(entityId)

        runOn(node4, node5, node6) {
          clusterReplication = createReplicationWith(typeName, raftActorProbe)
        }
        enterBarrier("ReplicationRegion created")

        runOn(node4) {
          clusterReplication ! ReplicationRegion.DeliverSomewhere(Command(message))
        }
        enterBarrier("message sent")

        runOn(node4, node5, node6) {
          val msgs = raftActorProbe.receiveWhile(max = 5.seconds, messages = 1) {
            case Command(msg: CheckRouting) => msg
          }
          controllerTestActor ! ReceivedMessages(msgs, myself, memberIndexes(myself))
        }

        runOn(controller) {
          val results: Map[MemberIndex, Seq[CheckRouting]] = receiveWhile(messages = 3) {
            case msg: ReceivedMessages => msg
          }.groupBy(_.memberIndex).map { case (role, m) => role -> m.flatMap(_.messages) }

          // 値を含んでいるのはいずれかの MemberIndex だけ
          val nonEmpty = results.filter { case (_, messages) => messages.nonEmpty }
          nonEmpty should have size 1
          nonEmpty.values.head shouldBe Seq(message)
        }
      }

      "ShardId が同じ場合は、ある role の同じノードに転送される" in {
        val typeName = createSeqTypeName()
        // entityId が同じなら ShardId も同じになる
        val entityId = createSeqEntityId()

        val message = CheckRouting(entityId)

        runOn(node4, node5, node6) {
          clusterReplication = createReplicationWith(typeName, raftActorProbe)
        }
        runOn(node4) {
          (1 to 3).foreach { _ =>
            clusterReplication ! message
          }
        }
        runOn(node4, node5, node6) {
          val msgs = raftActorProbe.receiveWhile(max = 5.seconds, messages = 3) {
            case Command(msg: CheckRouting) => msg
          }
          controllerTestActor ! ReceivedMessages(msgs, myself, memberIndexes(myself))
        }
        runOn(controller) {
          val results: Map[RoleName, Seq[CheckRouting]] = receiveWhile(messages = 3) {
            case msg: ReceivedMessages => msg
          }.groupBy(_.node)
            .filter { case (_, messages) => messages.nonEmpty }
            .map { case (node, m) => node -> m.flatMap(_.messages) }

          // 3ノードあり、その中で 3 通受け取っているのは 1 ノードだけ
          results.values should have size 3
          results.values.filter(v => v.size == 3) should have size 1
        }
      }

      "Adding a node has no effect on routing except for the MemberIndex with more nodes" in {
        val typeName = createSeqTypeName()
        // entityId が同じなら ShardId も同じになる
        val entityId = createSeqEntityId()

        val message = CheckRouting(entityId)

        runOn(node4, node5, node6) {
          clusterReplication = createReplicationWith(typeName, raftActorProbe)
        }
        runOn(node4) {
          (1 to 6).foreach { _ =>
            clusterReplication ! message
          }
        }
        runOn(node4, node5, node6) {
          val msgs = raftActorProbe.receiveWhile(max = 5.seconds, messages = 2) {
            case Command(msg: CheckRouting) => msg
          }
          controllerTestActor ! ReceivedMessages(msgs, myself, memberIndexes(myself))
        }
        var messagesBeforeNode4Joined: Map[RoleName, ReceivedMessages] = Map()
        runOn(controller) {
          receiveWhile(messages = 3) {
            case result: ReceivedMessages =>
              messagesBeforeNode4Joined += result.node -> result
          }
        }
        enterBarrier("message received")

        joinCluster(node7)
        runOn(node4) {
          (1 to 6).foreach { _ =>
            clusterReplication ! message
          }
        }
        runOn(node4, node5, node6, node7) {
          val msgs = raftActorProbe.receiveWhile(max = 5.seconds, messages = 2) {
            case Command(msg: CheckRouting) => msg
          }
          controllerTestActor ! ReceivedMessages(msgs, myself, memberIndexes(myself))
        }
        runOn(controller) {
          var messagesAfterNewNodeJoined: Map[RoleName, ReceivedMessages] = Map()
          receiveWhile(messages = 4) {
            case result: ReceivedMessages =>
              messagesAfterNewNodeJoined += result.node -> result
          }
          messagesBeforeNode4Joined.foreach {
            case (`node2`, before) => before.messages should be(messagesAfterNewNodeJoined(node2).messages)
            case (`node3`, before) => before.messages should be(messagesAfterNewNodeJoined(node3).messages)
            case _                 => // node1 and node4 can receive other messages by rebalancing
          }
        }
      }
    }
  }

  private[this] val idGenerator                 = new AtomicInteger(0)
  private[this] def createSeqEntityId(): String = s"replication-${idGenerator.incrementAndGet()}"
  private[this] def createSeqTypeName(): String = s"typeName-${idGenerator.incrementAndGet()}"

}
