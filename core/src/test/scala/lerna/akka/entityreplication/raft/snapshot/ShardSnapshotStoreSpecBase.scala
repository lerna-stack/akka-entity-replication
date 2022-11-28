package lerna.akka.entityreplication.raft.snapshot

import akka.persistence.testkit.{ PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin }
import com.typesafe.config.{ Config, ConfigFactory }

object ShardSnapshotStoreSpecBase {

  def configWithPersistenceTestKits: Config = {
    PersistenceTestKitPlugin.config
      .withFallback(PersistenceTestKitSnapshotPlugin.config)
      .withFallback(raftPersistenceConfigWithPersistenceTestKits)
      .withFallback(ConfigFactory.load())
  }

  private val raftPersistenceConfigWithPersistenceTestKits: Config = ConfigFactory.parseString(
    s"""
       |lerna.akka.entityreplication.raft.persistence {
       |  journal.plugin = ${PersistenceTestKitPlugin.PluginId}
       |  snapshot-store.plugin = ${PersistenceTestKitSnapshotPlugin.PluginId}
       |  # Might be possible to use PersistenceTestKitReadJournal
       |  // query.plugin = ""
       |}
       |""".stripMargin,
  )

}
