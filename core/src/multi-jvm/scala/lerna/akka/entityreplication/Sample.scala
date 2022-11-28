package lerna.akka.entityreplication

import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import com.typesafe.config.ConfigFactory

class SampleSpecMultiJvmNode1 extends SampleSpec
class SampleSpecMultiJvmNode2 extends SampleSpec

object SampleSpecConfig extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString("""
      akka.actor.provider = cluster
      """))
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
}

class SampleSpec extends MultiNodeSpec(SampleSpecConfig) with STMultiNodeSpec {
  import SampleSpecConfig._

  override def initialParticipants: Int = roles.size

  "fail" in {
    assert(false)
  }

  "Nodes" should {

    "wait for all nodes to enter a barrier" in {
      enterBarrier("startup")
    }

    "be able to say hello" in {
      runOn(node1) {
        val message = "Hello from node 1"
        message should be("Hello from node 1")
      }
      runOn(node2) {
        val message = "Hello from node 2"
        message should be("Hello from node 2")
      }
    }
  }
}
