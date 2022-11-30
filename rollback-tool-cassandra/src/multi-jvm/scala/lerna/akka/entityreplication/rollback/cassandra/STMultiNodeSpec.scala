package lerna.akka.entityreplication.rollback.cassandra

import akka.Done
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberRemoved, MemberUp }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeSpec, MultiNodeSpecCallbacks }
import akka.testkit.TestProbe
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.duration.DurationInt

/** ScalaTest with MultiNodeSpec */
trait STMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with Matchers with BeforeAndAfterAll {
  this: MultiNodeSpec =>

  val cluster: Cluster = Cluster(system)

  override def beforeAll(): Unit = {
    super.beforeAll()
    multiNodeSpecBeforeAll()
  }

  override def afterAll(): Unit = {
    try multiNodeSpecAfterAll()
    finally super.afterAll()
  }

  /** Creates a test probe to subscribe cluster events */
  def clusterEventProbe(): TestProbe = {
    val probe = TestProbe()
    cluster.subscribe(probe.ref, classOf[MemberUp], classOf[MemberRemoved])
    probe
  }

  /** Forms a new cluster consisting of the given nodes. */
  def newCluster(seedNode: RoleName, nodes: RoleName*): Unit = {
    val allNodes = seedNode +: nodes
    runOn(allNodes: _*) {
      val probe = clusterEventProbe()
      cluster.join(node(seedNode).address)
      probe.fishForSpecificMessage(max = 30.seconds) {
        case currentState: CurrentClusterState if currentState.members.map(_.address) == myAddress => Done
        case memberUp: MemberUp if memberUp.member.address == myAddress                            => Done
      }
    }
  }

}
