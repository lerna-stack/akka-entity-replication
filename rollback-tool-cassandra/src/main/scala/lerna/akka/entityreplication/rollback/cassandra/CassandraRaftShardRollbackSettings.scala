package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import com.typesafe.config.Config
import lerna.akka.entityreplication.rollback.RaftShardRollbackSettings

object CassandraRaftShardRollbackSettings {

  /** Creates a [[CassandraRaftShardRollbackSettings]] from the given system
    *
    * The root config instance for the settings is extracted from the config of the given system.
    * Config path `lerna.akka.entityreplication.rollback` is used for the extraction.
    *
    * @throws java.lang.IllegalArgumentException if the config contains an invalid setting value
    */
  def apply(system: ClassicActorSystemProvider): CassandraRaftShardRollbackSettings = {
    val config = system.classicSystem.settings.config.getConfig("lerna.akka.entityreplication.rollback")
    CassandraRaftShardRollbackSettings(system, config)
  }

  /** Creates a [[CassandraRaftShardRollbackSettings]] from the given system and config
    *
    * The given config should have the same structure at config path `lerna.akka.entityreplication.rollback`.
    *
    * @throws java.lang.IllegalArgumentException if the config contains an invalid setting value
    */
  def apply(system: ClassicActorSystemProvider, config: Config): CassandraRaftShardRollbackSettings = {
    val rollbackSettings =
      RaftShardRollbackSettings(config)
    val raftPersistencePluginLocation =
      config.getString("cassandra.raft-persistence-plugin-location")
    val raftEventSourcedPersistencePluginLocation =
      config.getString("cassandra.raft-eventsourced-persistence-plugin-location")
    new CassandraRaftShardRollbackSettings(
      system.classicSystem,
      rollbackSettings,
      raftPersistencePluginLocation,
      raftEventSourcedPersistencePluginLocation,
    )
  }

}

/** Settings for [[CassandraRaftShardRollback]] */
final class CassandraRaftShardRollbackSettings private (
    system: ActorSystem,
    val rollbackSettings: RaftShardRollbackSettings,
    val raftPersistencePluginLocation: String,
    val raftEventSourcedPersistencePluginLocation: String,
) {

  private[cassandra] val raftPersistenceRollbackSettings =
    CassandraPersistentActorRollbackSettings(
      system,
      raftPersistencePluginLocation,
      rollbackSettings.dryRun,
    )

  private[cassandra] val raftEventSourcedPersistenceRollbackSettings =
    CassandraPersistentActorRollbackSettings(
      system,
      raftEventSourcedPersistencePluginLocation,
      rollbackSettings.dryRun,
    )

}
