package lerna.akka.entityreplication

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ ActorRef, Props }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import akka.remote.testkit.MultiNodeSpec
import lerna.akka.entityreplication.ConsistencyTestBase.{ ConsistencyTestBaseConfig, ConsistencyTestReplicationActor }
import org.scalatest.Inside

class ConsistencyTestNormalMultiJvmNode1 extends ConsistencyTestNormal
class ConsistencyTestNormalMultiJvmNode2 extends ConsistencyTestNormal
class ConsistencyTestNormalMultiJvmNode3 extends ConsistencyTestNormal
class ConsistencyTestNormalMultiJvmNode4 extends ConsistencyTestNormal
class ConsistencyTestNormalMultiJvmNode5 extends ConsistencyTestNormal

class ConsistencyTestNormal extends MultiNodeSpec(ConsistencyTestBaseConfig) with STMultiNodeSpec with Inside {

  import ConsistencyTestBaseConfig._
  import lerna.akka.entityreplication.ConsistencyTestBase.ConsistencyTestReplicationActor._

  val generateUniqueId: () => String = {
    val counter = new AtomicInteger(0)
    () => {
      s"${myself.name}-${counter.getAndIncrement()}"
    }
  }

  private val nrOfNodes = roles.size

  override def initialParticipants: Int = roles.size

  var clusterReplication: ActorRef = null

  "準備" in {
    Cluster(system).subscribe(testActor, classOf[MemberUp])
    expectMsgType[CurrentClusterState]

    Cluster(system).join(node(node1).address)

    receiveN(nrOfNodes).map {
      case MemberUp(member) => member.address
    }.toSet should be(roles.map(node(_).address).toSet)

    Cluster(system).unsubscribe(testActor)

    enterBarrier("全nodeがClusterに参加した")

    clusterReplication = ClusterReplication(system).start(
      typeName = "sample",
      entityProps = Props[ConsistencyTestReplicationActor](),
      settings = ClusterReplicationSettings(system),
      extractEntityId = ConsistencyTestReplicationActor.extractEntityId,
      extractShardId = ConsistencyTestReplicationActor.extractShardId,
    )
  }

  "正常系（直列に処理した場合）" should {

    val entityId = "正常系（直列に処理した場合）"

    "node1宛にCountUp" in {
      runOn(node1) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
    }

    "check count 1" in {
      val count     = 1
      val requestId = generateUniqueId()
      clusterReplication ! GetStatus(entityId, requestId)
      inside(expectMsgType[Status]) {
        case status =>
          status.requestId should be(requestId)
          status.count should be(count)
      }
    }

    "node2宛にCountUp" in {
      runOn(node2) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
    }

    "check count 2" in {
      val count     = 2
      val requestId = generateUniqueId()
      clusterReplication ! GetStatus(entityId, requestId)
      inside(expectMsgType[Status]) {
        case status =>
          status.requestId should be(requestId)
          status.count should be(count)
      }
    }

    "node3宛にCountUp" in {
      runOn(node3) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
    }

    "check count 3" in {
      val count     = 3
      val requestId = generateUniqueId()
      clusterReplication ! GetStatus(entityId, requestId)
      inside(expectMsgType[Status]) {
        case status =>
          status.requestId should be(requestId)
          status.count should be(count)
      }
    }
  }

  "正常系（並列に処理した場合）" should {

    val entityId = "正常系（並列に処理した場合）"

    "全node宛にCountUp" in {
      runOn(node1) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
      runOn(node2) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
      runOn(node3) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
      runOn(node4) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
      runOn(node5) {
        val requestId = generateUniqueId()
        clusterReplication ! CountUp(entityId, requestId)
        expectMsgType[Complete].requestId should be(requestId)
      }
    }

    "check count" in {
      val count     = 5
      val requestId = generateUniqueId()
      clusterReplication ! GetStatus(entityId, requestId)
      inside(expectMsgType[Status]) {
        case status =>
          status.requestId should be(requestId)
          status.count should be(count)
      }
    }
  }
}
