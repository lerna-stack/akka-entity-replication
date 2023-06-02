package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.Config

private object CassandraSnapshotSettings {

  /** Creates a [[CassandraSnapshotSettings]] from the given config
    *
    * The given config should have the same structure as the one of Akka Persistence Cassandra plugin
    * (`akka.persistence.cassandra`).
    */
  def apply(pluginConfig: Config): CassandraSnapshotSettings = {
    val readProfile: String =
      pluginConfig.getString("snapshot.read-profile")
    val writeProfile: String =
      pluginConfig.getString("snapshot.write-profile")
    val keyspace: String =
      pluginConfig.getString("snapshot.keyspace")
    val table: String =
      pluginConfig.getString("snapshot.table")
    new CassandraSnapshotSettings(
      readProfile,
      writeProfile,
      keyspace,
      table,
    )
  }

}

private final class CassandraSnapshotSettings private (
    val readProfile: String,
    val writeProfile: String,
    val keyspace: String,
    val table: String,
) {

  /** The table name qualified with the keyspace name */
  def tableName: String = s"${keyspace}.${table}"

}
