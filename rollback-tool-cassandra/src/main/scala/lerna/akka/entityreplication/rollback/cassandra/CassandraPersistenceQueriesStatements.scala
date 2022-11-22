package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem

private final class CassandraPersistenceQueriesStatements(
    system: ActorSystem,
    settings: CassandraPersistenceQueriesSettings,
) {

  private val journalSettings: CassandraJournalSettings =
    settings.resolveJournalSettings(system)

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

}
