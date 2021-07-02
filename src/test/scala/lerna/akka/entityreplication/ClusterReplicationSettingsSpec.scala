package lerna.akka.entityreplication

import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.internal.ClusterReplicationSettingsImpl
import org.scalatest.{ Matchers, WordSpec }

class ClusterReplicationSettingsSpec extends WordSpec with Matchers {

  private val config: Config = ConfigFactory
    .parseString("""
      |lerna.akka.entityreplication.raft.multi-raft-roles = ["group-1", "group-2", "group-3"]
      |""".stripMargin).withFallback(ConfigFactory.load())

  "ClusterReplicationSettings" should {

    "not throw any exceptions when clusterRoles is appropriate" in {
      val clusterRoles: Set[String] = Set("dummy", "group-1") // one of the multi-raft-roles is included

      ClusterReplicationSettingsImpl(config, clusterRoles) // no thrown
    }

    "throw an exception when does not include any of the multi-raft-roles" in {
      val clusterRoles: Set[String] = Set("dummy")

      val exception = intercept[IllegalStateException] {
        ClusterReplicationSettingsImpl(config, clusterRoles)
      }
      exception.getMessage should be("requires one of Set(group-1, group-2, group-3) role")
    }

    "throw an exception when contains two or more of multi-raft-roles" in {
      val clusterRoles: Set[String] = Set("dummy", "group-1", "group-2")

      val exception = intercept[IllegalStateException] {
        ClusterReplicationSettingsImpl(config, clusterRoles)
      }
      exception.getMessage should be(
        "requires one of Set(group-1, group-2, group-3) role, should not have multiply roles: [group-1,group-2]",
      )
    }

    val correctClusterRoles = Set("group-1", "group-2", "group-3")

    "throw an exception when number-of-shards less than or equal to 0" in {
      val cfg = ConfigFactory
        .parseString("""
          lerna.akka.entityreplication.raft.number-of-shards = 0
          """).withFallback(config)

      val exception = intercept[IllegalArgumentException] {
        ClusterReplicationSettingsImpl(cfg, correctClusterRoles)
      }
      exception.getMessage should be(
        "requirement failed: number-of-shards (0) should be larger than 0",
      )
    }

    "change value of raftSettings.journalPluginId by withRaftJournalPluginId" in {
      val settings         = ClusterReplicationSettingsImpl(config, correctClusterRoles.headOption.toSet)
      val expectedPluginId = "new-raft-journal-plugin-id"
      val modifiedSettings = settings.withRaftJournalPluginId(expectedPluginId)
      modifiedSettings.raftSettings.journalPluginId should be(expectedPluginId)
    }

    "return config which contains settings of journal-plugin-additional by raftSettings.journalPluginAdditionalConfig after overriding RaftJournalPluginId" in {
      val localConfig = ConfigFactory
        .parseString("""
        lerna.akka.entityreplication.raft.persistence.journal-plugin-additional {
          additional-setting = "ok"
        }               
        """).withFallback(config)
      val settings         = ClusterReplicationSettingsImpl(localConfig, correctClusterRoles.headOption.toSet)
      val expectedPluginId = "new-raft-journal-plugin-id"
      val modifiedSettings = settings.withRaftJournalPluginId(expectedPluginId)
      modifiedSettings.raftSettings.journalPluginAdditionalConfig.getString(
        "new-raft-journal-plugin-id.additional-setting",
      ) should be("ok")
    }

    "change value of raftSettings.snapshotStorePluginId by withRaftSnapshotPluginId" in {
      val settings         = ClusterReplicationSettingsImpl(config, correctClusterRoles.headOption.toSet)
      val expectedPluginId = "new-raft-snapshot-plugin-id"
      val modifiedSettings = settings.withRaftSnapshotPluginId(expectedPluginId)
      modifiedSettings.raftSettings.snapshotStorePluginId should be(expectedPluginId)
    }

    "change value of raftSettings.queryPluginId by withRaftQueryPluginId" in {
      val settings         = ClusterReplicationSettingsImpl(config, correctClusterRoles.headOption.toSet)
      val expectedPluginId = "new-raft-query-plugin-id"
      val modifiedSettings = settings.withRaftQueryPluginId(expectedPluginId)
      modifiedSettings.raftSettings.queryPluginId should be(expectedPluginId)
    }

    "change value of raftSettings.eventSourcedJournalPluginId by withEventSourcedJournalPluginId" in {
      val settings         = ClusterReplicationSettingsImpl(config, correctClusterRoles.headOption.toSet)
      val expectedPluginId = "new-event-sourced-journal-plugin-id"
      val modifiedSettings = settings.withEventSourcedJournalPluginId(expectedPluginId)
      modifiedSettings.raftSettings.eventSourcedJournalPluginId should be(expectedPluginId)
    }
  }
}
