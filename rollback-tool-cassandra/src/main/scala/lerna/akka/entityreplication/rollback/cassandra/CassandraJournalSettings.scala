package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.Config

private object CassandraJournalSettings {

  /** Creates a [[CassandraJournalSettings]] from the given config
    *
    * The given config should have the same structure as the one of Akka Persistence Cassandra plugin
    * (`akka.persistence.cassandra`).
    *
    * @throws java.lang.IllegalArgumentException if the given config contains an invalid setting value
    */
  def apply(pluginConfig: Config): CassandraJournalSettings = {
    val readProfile =
      pluginConfig.getString("read-profile")
    val writeProfile =
      pluginConfig.getString("write-profile")
    val keyspace =
      pluginConfig.getString("journal.keyspace")
    val table =
      pluginConfig.getString("journal.table")
    val metadataTable =
      pluginConfig.getString("journal.metadata-table")
    val targetPartitionSize =
      pluginConfig.getLong("journal.target-partition-size")
    new CassandraJournalSettings(
      readProfile,
      writeProfile,
      keyspace,
      table,
      metadataTable,
      targetPartitionSize,
    )
  }

}

private final class CassandraJournalSettings private (
    val readProfile: String,
    val writeProfile: String,
    val keyspace: String,
    val table: String,
    val metadataTable: String,
    val targetPartitionSize: Long,
) {
  require(
    targetPartitionSize > 0,
    s"journal.target-partition-size [$targetPartitionSize] should be greater than 0",
  )

  /** The table name qualified with the keyspace name */
  def tableName: String = s"${keyspace}.${table}"

  /** The metadata table name qualified with the keyspace name */
  def metadataTableName: String = s"${keyspace}.${metadataTable}"

}
