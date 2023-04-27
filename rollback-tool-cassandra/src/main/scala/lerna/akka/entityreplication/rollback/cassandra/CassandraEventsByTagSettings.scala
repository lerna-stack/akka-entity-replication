package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.Config

private object CassandraEventsByTagSettings {

  /** Creates a [[CassandraEventsByTagSettings]] from the given config
    *
    * The given config should have the same structure as the one of Akka Persistence Cassandra plugin (`akka.persistence.cassandra`).
    */
  def apply(pluginConfig: Config): CassandraEventsByTagSettings = {
    val keyspace =
      pluginConfig.getString("journal.keyspace")
    val table =
      pluginConfig.getString("events-by-tag.table")
    new CassandraEventsByTagSettings(keyspace, table)
  }

}

private final class CassandraEventsByTagSettings private (
    val keyspace: String,
    val table: String,
) {

  /** The tag_views table name qualified with the keyspace name */
  def tagViewsTableName: String = s"${keyspace}.${table}"

  /** The tag_write_progress table name qualified with the keyspace name */
  def tagWriteProgressTableName: String = s"${keyspace}.tag_write_progress"

  /** The tag_scanning table name qualified with the keyspace name */
  def tagScanningTableName: String = s"${keyspace}.tag_scanning"

}
