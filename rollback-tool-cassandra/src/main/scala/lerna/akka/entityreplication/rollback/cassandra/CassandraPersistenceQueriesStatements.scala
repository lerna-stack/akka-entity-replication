package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem

private final class CassandraPersistenceQueriesStatements(
    system: ActorSystem,
    settings: CassandraPersistenceQueriesSettings,
) {

  private val journalSettings: CassandraJournalSettings =
    settings.resolveJournalSettings(system)
  private val snapshotSettings: CassandraSnapshotSettings =
    settings.resolveSnapshotSettings(system)

  val selectHighestSequenceNr: String =
    s"""
       SELECT sequence_nr FROM ${journalSettings.tableName}
       WHERE
         persistence_id = ? AND
         partition_nr = ?
       ORDER BY sequence_nr DESC
       LIMIT 1
       """

  val selectMessagesFromAsc: String =
    s"""
       SELECT * FROM ${journalSettings.tableName}
       WHERE
         persistence_id = ? AND
         partition_nr = ? AND
         sequence_nr >= ?
       ORDER BY sequence_nr ASC
       """

  val selectMessagesFromDesc: String =
    s"""
       SELECT * FROM ${journalSettings.tableName}
       WHERE
         persistence_id = ? AND
         partition_nr = ? AND
         sequence_nr <= ?
       ORDER BY sequence_nr DESC
       """

  val selectDeletedTo: String =
    s"""
       SELECT deleted_to FROM ${journalSettings.metadataTableName}
       WHERE
         persistence_id = ?
     """

  val selectLowestSnapshotSequenceNrFrom: String =
    s"""
       SELECT sequence_nr FROM ${snapshotSettings.tableName}
       WHERE
         persistence_id = ? AND
         sequence_nr >= ?
       ORDER BY sequence_nr ASC
       LIMIT 1
       """

}
