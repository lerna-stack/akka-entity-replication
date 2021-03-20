package lerna.akka.entityreplication

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpec }

class ClusterReplicationSettingsSpec extends WordSpec with Matchers {

  private val config: Config = ConfigFactory
    .parseString("""
      |lerna.akka.entityreplication.raft.multi-raft-roles = ["group-1", "group-2", "group-3"]
      |""".stripMargin).withFallback(ConfigFactory.load())

  "ClusterReplicationSettings" when {

    "instantiate" should {

      "not throw any exceptions when clusterRoles is appropriate" in {
        val clusterRoles: Set[String] = Set("dummy", "group-1") // one of the multi-raft-roles is included

        new ClusterReplicationSettings(config, clusterRoles) // no thrown
      }

      "throw an exception when does not include any of the multi-raft-roles" in {
        val clusterRoles: Set[String] = Set("dummy")

        val exception = intercept[IllegalStateException] {
          new ClusterReplicationSettings(config, clusterRoles)
        }
        exception.getMessage should be("requires one of Set(group-1, group-2, group-3) role")
      }

      "throw an exception when contains two or more of multi-raft-roles" in {
        val clusterRoles: Set[String] = Set("dummy", "group-1", "group-2")

        val exception = intercept[IllegalStateException] {
          new ClusterReplicationSettings(config, clusterRoles)
        }
        exception.getMessage should be(
          "requires one of Set(group-1, group-2, group-3) role, should not have multiply roles: [group-1,group-2]",
        )
      }
    }

  }
}
