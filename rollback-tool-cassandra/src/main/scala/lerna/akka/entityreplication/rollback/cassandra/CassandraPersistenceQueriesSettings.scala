package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem

/** Settings of [[CassandraPersistenceQueries]] */
private final class CassandraPersistenceQueriesSettings(
    val pluginLocation: String,
) {

  /** Resolves `pluginLocation` on the given system and then returns the journal plugin settings */
  def resolveJournalSettings(system: ActorSystem): CassandraJournalSettings = {
    val pluginConfig = system.settings.config.getConfig(pluginLocation)
    CassandraJournalSettings(pluginConfig)
  }

  /** Resolves `pluginLocation` on the given system and then returns the query plugin settings */
  def resolveQuerySettings(system: ActorSystem): CassandraQuerySettings = {
    val pluginConfig = system.settings.config.getConfig(pluginLocation)
    CassandraQuerySettings(pluginConfig)
  }

  /** Resolves `pluginLocation` on the given system and then returns the snapshot plugin settings */
  def resolveSnapshotSettings(system: ActorSystem): CassandraSnapshotSettings = {
    val pluginConfig = system.settings.config.getConfig(pluginLocation)
    CassandraSnapshotSettings(pluginConfig)
  }

}
