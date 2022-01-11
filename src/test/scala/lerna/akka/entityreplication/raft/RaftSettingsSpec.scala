package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration.DurationInt

final class RaftSettingsSpec extends TestKit(ActorSystem("RaftSettingsSpec")) with ActorSpec {

  private val defaultConfig: Config = {
    // Use reference.conf directly.
    // Some tests should verify default values that is not overwritten values for tests.
    ConfigFactory.load("reference.conf")
  }

  "RaftSettings" should {

    "load the default settings" in {
      val settings = RaftSettings(defaultConfig)
      settings.config shouldBe defaultConfig.getConfig("lerna.akka.entityreplication.raft")
      settings.electionTimeout shouldBe 750.millis
      settings.heartbeatInterval shouldBe 100.millis
      settings.multiRaftRoles shouldBe Set("replica-group-1", "replica-group-2", "replica-group-3")
      settings.replicationFactor shouldBe 3
      settings.quorumSize shouldBe 2
      settings.numberOfShards shouldBe 100
      settings.maxAppendEntriesSize shouldBe 16
      settings.maxAppendEntriesBatchSize shouldBe 10
      settings.compactionSnapshotCacheTimeToLive shouldBe 10.seconds
      settings.compactionLogSizeThreshold shouldBe 50_000
      settings.compactionPreserveLogSize shouldBe 10_000
      settings.compactionLogSizeCheckInterval shouldBe 10.seconds
      settings.snapshotSyncCopyingParallelism shouldBe 10
      settings.snapshotSyncPersistenceOperationTimeout shouldBe 10.seconds
      settings.clusterShardingConfig shouldBe defaultConfig.getConfig("lerna.akka.entityreplication.raft.sharding")
      settings.journalPluginId shouldBe ""
      settings.snapshotStorePluginId shouldBe ""
      settings.queryPluginId shouldBe ""
      settings.eventSourcedJournalPluginId shouldBe ""
      settings.eventSourcedSnapshotStorePluginId shouldBe ""
      settings.eventSourcedSnapshotEvery shouldBe 1_000
    }

    "load the default journalPluginAdditionalConfig with non-empty journalPluginId" in {
      val config = ConfigFactory
        .parseString("""
          |lerna.akka.entityreplication.raft.persistence.journal.plugin = my-journal-plugin-id
          |""".stripMargin)
        .withFallback(defaultConfig)
      val settings = RaftSettings(config)
      settings.journalPluginAdditionalConfig.getConfig("my-journal-plugin-id") shouldBe defaultConfig.getConfig(
        "lerna.akka.entityreplication.raft.persistence.journal-plugin-additional",
      )
    }

    "throw an IllegalArgumentException if the given snapshot-every is out of range" in {
      val config = ConfigFactory
        .parseString("""
            |lerna.akka.entityreplication.raft.eventsourced.persistence.snapshot-every = 0
            |""".stripMargin)
        .withFallback(defaultConfig)
      a[IllegalArgumentException] shouldBe thrownBy {
        RaftSettings(config)
      }
    }

    "create new settings using withJournalPluginId" in {
      val settings    = RaftSettings(defaultConfig)
      val newSettings = settings.withJournalPluginId("new-journal-plugin-id")
      newSettings.journalPluginId shouldNot be(settings.journalPluginId)
      newSettings.journalPluginId shouldBe "new-journal-plugin-id"
    }

    "create new settings using withSnapshotPluginId" in {
      val settings    = RaftSettings(defaultConfig)
      val newSettings = settings.withSnapshotPluginId("new-snapshot-store-plugin-id")
      newSettings.snapshotStorePluginId shouldNot be(settings.snapshotStorePluginId)
      newSettings.snapshotStorePluginId shouldBe "new-snapshot-store-plugin-id"
    }

    "create new settings using withQueryPluginId" in {
      val settings    = RaftSettings(defaultConfig)
      val newSettings = settings.withQueryPluginId("new-query-plugin-id")
      newSettings.queryPluginId shouldNot be(settings.queryPluginId)
      newSettings.queryPluginId shouldBe "new-query-plugin-id"
    }

    "create new settings using withEventSourcedJournalPluginId" in {
      val settings    = RaftSettings(defaultConfig)
      val newSettings = settings.withEventSourcedJournalPluginId("new-eventsourced-journal-plugin-id")
      newSettings.eventSourcedJournalPluginId shouldNot be(settings.eventSourcedJournalPluginId)
      newSettings.eventSourcedJournalPluginId shouldBe "new-eventsourced-journal-plugin-id"
    }

    "create new settings using withEventSourcedSnapshotStorePluginId" in {
      val settings    = RaftSettings(defaultConfig)
      val newSettings = settings.withEventSourcedSnapshotStorePluginId("new-eventsourced-snapshot-store-plugin-id")
      newSettings.eventSourcedSnapshotStorePluginId shouldNot be(settings.eventSourcedSnapshotStorePluginId)
      newSettings.eventSourcedSnapshotStorePluginId shouldBe "new-eventsourced-snapshot-store-plugin-id"
    }

  }

}