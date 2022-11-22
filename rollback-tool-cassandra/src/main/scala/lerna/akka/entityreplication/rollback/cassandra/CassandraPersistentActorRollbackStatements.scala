package lerna.akka.entityreplication.rollback.cassandra

private final class CassandraPersistentActorRollbackStatements(settings: CassandraPersistentActorRollbackSettings) {

  object journal {
    val deleteMessagesFrom: String =
      s"""
       DELETE FROM ${settings.journal.tableName}
       WHERE
         persistence_id = ? AND
         partition_nr = ? AND
         sequence_nr >= ?
       """
  }

  object snapshot {
    val deleteSnapshotsFrom: String =
      s"""
       DELETE FROM ${settings.snapshot.tableName}
       WHERE
         persistence_id = ? AND
         sequence_nr >= ?
       """
  }

}
