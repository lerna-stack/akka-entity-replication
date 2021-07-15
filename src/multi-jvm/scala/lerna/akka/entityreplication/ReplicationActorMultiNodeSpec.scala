package lerna.akka.entityreplication

import java.util.concurrent.atomic.AtomicInteger
import akka.{ Done, NotUsed }
import akka.actor.{ ActorRef, PoisonPill, Props }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.collection.Set

object ReplicationActorSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-2"),
    node3 -> MemberIndex("member-3"),
  )

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      lerna.akka.entityreplication.raft.multi-raft-roles = ["member-1", "member-2", "member-3"]
      // 1 イベントごとに snapshot が取得されるようにする
      lerna.akka.entityreplication.raft.compaction.log-size-threshold = 1
      lerna.akka.entityreplication.raft.compaction.preserve-log-size = 1
      lerna.akka.entityreplication.raft.compaction.log-size-check-interval = 0.1s
      lerna.akka.entityreplication.recovery-entity-timeout = 1s
      """))
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node1)}"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2)}"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node3)}"]
  """))
}

object ReplicationActorMultiNodeSpec {

  object PingPongReplicationActor {

    sealed trait Command extends STMultiNodeSerializable {
      def id: String
    }
    case class Ping(id: String)             extends Command
    case class Pong(id: String, count: Int) extends STMultiNodeSerializable
    case class Break(id: String)            extends Command

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }
    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }
  }

  @nowarn // for deprecated ReplicationActor
  class PingPongReplicationActor extends ReplicationActor[Int] {

    import PingPongReplicationActor._

    var count: Int = 0

    override def preStart(): Unit = {
      println(s"=== ${context.self.path} started ===")
    }

    override def receiveReplica: Receive = {
      case SnapshotOffer(snapshot: Int) =>
        count = snapshot
      case Ping(_) => updateState()
    }

    override def receiveCommand: Receive = {
      case ping: Ping =>
        replicate(ping) {
          case Ping(id) =>
            updateState()
            sender() ! Pong(id, count)
        }
      case _: Break =>
        throw new RuntimeException("break!")
    }

    def updateState(): Unit = {
      count += 1
    }

    override def currentState: Int = count
  }

  object LockReplicationActor {

    sealed trait Command extends STMultiNodeSerializable {
      def id: String
    }
    case class Lock(id: String)   extends Command
    case class UnLock(id: String) extends Command

    case class GetStatus(id: String)                extends Command
    case class Status(id: String, locking: Boolean) extends STMultiNodeSerializable

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => ((c.id, c))
    }
    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }
  }

  @nowarn // for deprecated ReplicationActor
  class LockReplicationActor extends ReplicationActor[NotUsed] {
    import LockReplicationActor._

    override def preStart(): Unit = {
      println(s"=== ${context.self.path} started ===")
    }

    override def receiveReplica: Receive = {
      case Lock(_) =>
        context.become(lock)
      case UnLock(_) =>
        context.become(unlock)
    }

    override def receiveCommand: Receive = unlock // init state

    def lock: Receive = {
      case evt: UnLock =>
        replicate(evt) { _ =>
          context.become(unlock)
        }
      case GetStatus(id) =>
        sender() ! Status(id, locking = true)
    }

    def unlock: Receive = {
      case evt: Lock =>
        replicate(evt) { _ =>
          context.become(lock)
        }
      case GetStatus(id) =>
        sender() ! Status(id, locking = false)
    }

    // 簡単のため Snapshot は無効化
    override def currentState: NotUsed = NotUsed
  }

  object EphemeralReplicationActor {

    sealed trait Command extends STMultiNodeSerializable {
      def id: String
    }

    case class Start(id: String)          extends Command
    case class Stop(id: String)           extends Command
    case class IncrementCount(id: String) extends Command
    case class GetState(id: String)       extends Command

    case class State(count: Int) extends STMultiNodeSerializable

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }
    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }
  }

  @nowarn // for deprecated ReplicationActor
  class EphemeralReplicationActor extends ReplicationActor[Int] {

    import EphemeralReplicationActor._

    override def receiveReplica: Receive = {
      case SnapshotOffer(snapshot: Int) => count = snapshot
      case increment: IncrementCount    => updateState(increment)
    }

    override def receiveCommand: Receive = {
      case Start(_) => // ignore
      case Stop(_)  => context.parent ! ReplicationRegion.Passivate(self.path, PoisonPill)
      case increment: IncrementCount =>
        replicate(increment) { e =>
          updateState(e)
        }
      case _: GetState =>
        ensureConsistency {
          sender() ! State(count)
        }
    }

    var count = 0

    def updateState(event: IncrementCount): Unit =
      event match {
        case _: IncrementCount => count += 1
      }

    override def currentState: Int = count
  }
}

class ReplicationActorMultiNodeSpecMultiJvmNode1 extends ReplicationActorMultiNodeSpec
class ReplicationActorMultiNodeSpecMultiJvmNode2 extends ReplicationActorMultiNodeSpec
class ReplicationActorMultiNodeSpecMultiJvmNode3 extends ReplicationActorMultiNodeSpec

@nowarn // for deprecated ClusterReplication(system).start
class ReplicationActorMultiNodeSpec extends MultiNodeSpec(ReplicationActorSpecConfig) with STMultiNodeSpec {
  import ReplicationActorMultiNodeSpec._
  import ReplicationActorSpecConfig._

  "ReplicationActor" should {

    "wait for all nodes to enter a barrier" in {

      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgType[CurrentClusterState]

      Cluster(system).join(node(node1).address)

      receiveN(3).collect {
        case MemberUp(member) => member.address
      }.toSet should be(Set(node(node1).address, node(node2).address, node(node3).address))

      Cluster(system).unsubscribe(testActor)

      enterBarrier("started up a cluster")
    }

    "イベントのレプリケーション後に sender() を使って応答を返せる" in {
      import PingPongReplicationActor._

      var clusterReplication: ActorRef = null

      runOn(node1, node2, node3) {
        clusterReplication = planAutoKill {
          ClusterReplication(system).start(
            typeName = "ping-pong-sample",
            entityProps = Props[PingPongReplicationActor](),
            settings = ClusterReplicationSettings(system),
            extractEntityId = PingPongReplicationActor.extractEntityId,
            extractShardId = PingPongReplicationActor.extractShardId,
          )
        }
      }

      val entityId = createSeqEntityId()

      runOn(node1) {
        clusterReplication ! Ping(entityId)
        expectMsg(Pong(entityId, count = 1))
        clusterReplication ! Ping(entityId)
        expectMsg(Pong(entityId, count = 2))
        clusterReplication ! Ping(entityId)
        expectMsg(Pong(entityId, count = 3))
      }
    }

    "be able to continue processing commands even if an exception occurred" in {
      import PingPongReplicationActor._

      var clusterReplication: ActorRef = null

      runOn(node1, node2, node3) {
        clusterReplication = planAutoKill {
          ClusterReplication(system).start(
            typeName = "ping-pong-sample-2",
            entityProps = Props[PingPongReplicationActor](),
            settings = ClusterReplicationSettings(system),
            extractEntityId = PingPongReplicationActor.extractEntityId,
            extractShardId = PingPongReplicationActor.extractShardId,
          )
        }
      }

      val entityId = createSeqEntityId()

      runOn(node1) {
        clusterReplication ! Ping(entityId)
        expectMsg(Pong(entityId, count = 1))
        clusterReplication ! Ping(entityId)
        expectMsg(Pong(entityId, count = 2))
        clusterReplication ! Break(entityId)
        awaitAssert {
          clusterReplication ! Ping(entityId)
          fishForSpecificMessage(max = 1.seconds) {
            case Pong(`entityId`, count) if count >= 3 => Done
          }
        }
      }
    }

    "context.become で状態遷移できる" in {
      import LockReplicationActor._

      var clusterReplication: ActorRef = null

      runOn(node1, node2, node3) {
        clusterReplication = planAutoKill {
          ClusterReplication(system).start(
            typeName = "lock-sample",
            entityProps = Props[LockReplicationActor](),
            settings = ClusterReplicationSettings(system),
            extractEntityId = LockReplicationActor.extractEntityId,
            extractShardId = LockReplicationActor.extractShardId,
          )
        }
      }

      val entityId = createSeqEntityId()

      runOn(node1) {
        // 初期状態はロックされていない
        clusterReplication ! GetStatus(entityId)
        expectMsg(Status(entityId, locking = false))
        // ロックする
        clusterReplication ! Lock(entityId)
        clusterReplication ! GetStatus(entityId)
        expectMsg(Status(entityId, locking = true))
        // ロックを解除する
        clusterReplication ! UnLock(entityId)
        clusterReplication ! GetStatus(entityId)
        expectMsg(Status(entityId, locking = false))
      }
    }

    "Passivate で全てのレプリカが終了する" in {
      import EphemeralReplicationActor._

      var clusterReplication: ActorRef = null
      var raftMember: ActorRef         = null

      val entityId = createSeqEntityId()

      runOn(node1, node2, node3) {
        clusterReplication = planAutoKill {
          ClusterReplication(system).start(
            typeName = "passivate-sample",
            entityProps = Props[EphemeralReplicationActor](),
            settings = ClusterReplicationSettings(system),
            extractEntityId = EphemeralReplicationActor.extractEntityId,
            extractShardId = EphemeralReplicationActor.extractShardId,
          )
        }
      }
      runOn(node1) {
        clusterReplication ! Start(entityId)
      }
      runOn(node1, node2, node3) {
        awaitAssert {
          // なるべく早くリトライ
          implicit val timeout: Timeout = Timeout(0.25.seconds)
          raftMember = watch(
            system.actorSelection(s"/system/sharding/raft-shard-passivate-sample-*/*/*/$entityId").resolveOne().await,
          )
        }
      }
      enterBarrier("raft members created")

      runOn(node1) {
        clusterReplication ! Stop(entityId)
      }

      runOn(node1, node2, node3) {
        expectTerminated(raftMember)
      }
    }
  }

  "一度 ReplicationActor が停止しても、コマンドを処理する前に状態が復元される" in {
    import EphemeralReplicationActor._

    var clusterReplication: ActorRef = null

    runOn(node1, node2, node3) {
      clusterReplication = planAutoKill {
        ClusterReplication(system).start(
          typeName = "recovery-sample",
          entityProps = Props[EphemeralReplicationActor](),
          settings = ClusterReplicationSettings(system),
          extractEntityId = EphemeralReplicationActor.extractEntityId,
          extractShardId = EphemeralReplicationActor.extractShardId,
        )
      }
    }

    val entityId = createSeqEntityId()

    runOn(node1) {
      // 初期値は 0
      clusterReplication ! GetState(entityId)
      expectMsg(State(0))
      // インクリメントする
      clusterReplication ! IncrementCount(entityId)
      clusterReplication ! GetState(entityId)
      expectMsg(State(1))
    }

    var raftMember: ActorRef = null
    runOn(node1, node2, node3) {
      awaitAssert {
        // なるべく早くリトライ
        implicit val timeout: Timeout = Timeout(0.25.seconds)
        raftMember = watch(
          system
            .actorSelection(
              s"/system/sharding/raft-shard-recovery-sample-${memberIndexes(myself)}/*/*/$entityId",
            ).resolveOne().await,
        )
      }
    }
    enterBarrier("raft members found")
    runOn(node1) {
      // ReplicationActor を停止する
      clusterReplication ! Stop(entityId)
    }
    runOn(node1, node2, node3) {
      expectTerminated(raftMember)
    }
    enterBarrier("raft members terminated")
    runOn(node1) {
      // 状態が復元されている
      clusterReplication ! GetState(entityId)
      expectMsg(State(1))
    }
  }

  private[this] val idGenerator                 = new AtomicInteger(0)
  private[this] def createSeqEntityId(): String = s"replication-${idGenerator.incrementAndGet()}"

}
