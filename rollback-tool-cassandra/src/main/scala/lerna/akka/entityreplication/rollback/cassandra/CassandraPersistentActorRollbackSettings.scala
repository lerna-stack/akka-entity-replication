package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem
import akka.persistence.cassandra.cleanup.CleanupSettings
import akka.persistence.cassandra.reconciler.ReconciliationSettings
import com.typesafe.config.{ Config, ConfigFactory }

private object CassandraPersistentActorRollbackSettings {

  /** Creates a [[CassandraPersistentActorRollbackSettings]] from the given system
    *
    * `pluginLocation` is an absolute config path to specify an Akka Persistence Cassandra plugin config. The Akka
    * Persistence Cassandra plugin config is resolved by using both `pluginLocation` and the given system.
    */
  def apply(
      system: ActorSystem,
      pluginLocation: String,
      dryRun: Boolean,
  ): CassandraPersistentActorRollbackSettings = {
    new CassandraPersistentActorRollbackSettings(system, pluginLocation, dryRun)
  }

}

private final class CassandraPersistentActorRollbackSettings private (
    system: ActorSystem,
    val pluginLocation: String,
    val dryRun: Boolean,
) {

  private val pluginConfig: Config =
    system.settings.config.getConfig(pluginLocation)

  private[cassandra] val journal: CassandraJournalSettings =
    CassandraJournalSettings(pluginConfig)

  private[cassandra] val query: CassandraQuerySettings =
    CassandraQuerySettings(pluginConfig)

  private[cassandra] val snapshot: CassandraSnapshotSettings =
    CassandraSnapshotSettings(pluginConfig)

  private[cassandra] val cleanup: CleanupSettings = {
    val baseConfig = pluginConfig.getConfig("cleanup")
    val cleanupConfig: Config = ConfigFactory
      .parseString(
        s"""
        |plugin-location = "$pluginLocation"
        |dry-run = $dryRun
        |""".stripMargin,
      ).withFallback(baseConfig)
    new CleanupSettings(cleanupConfig)
  }

  private[cassandra] val reconciliation: ReconciliationSettings = {
    val baseConfig = pluginConfig.getConfig("reconciler")
    val reconciliationConfig: Config = ConfigFactory
      .parseString(
        s"""
        |read-profile = "${journal.readProfile}"
        |write-profile = "${journal.writeProfile}"
        |plugin-location = "$pluginLocation"
        |""".stripMargin,
      ).withFallback(baseConfig)
    new ReconciliationSettings(reconciliationConfig)
  }

  private[cassandra] val queries: CassandraPersistenceQueriesSettings =
    new CassandraPersistenceQueriesSettings(pluginLocation)

}
