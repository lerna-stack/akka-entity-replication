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

    val deleteTagViews: String =
      s"""
       DELETE FROM ${settings.eventsByTag.tagViewsTableName}
       WHERE
         tag_name = ? AND
         timebucket = ? AND
         timestamp = ? AND
         persistence_id = ? AND
         tag_pid_sequence_nr = ?
       """

    val insertTagWriteProgress: String =
      s"""
       INSERT INTO ${settings.eventsByTag.tagWriteProgressTableName}
       (persistence_id, tag, sequence_nr, tag_pid_sequence_nr, offset)
       VALUES (?, ?, ?, ?, ?)
       """

    val insertTagScanning: String =
      s"""
       INSERT INTO ${settings.eventsByTag.tagScanningTableName}
       (persistence_id, sequence_nr)
       VALUES (?, ?)
       """

    val deleteTagWriteProgress: String =
      s"""
       DELETE FROM ${settings.eventsByTag.tagWriteProgressTableName}
       WHERE
         persistence_id = ? AND
         tag = ?
       """

    val deleteTagScanning: String =
      s"""
       DELETE FROM ${settings.eventsByTag.tagScanningTableName}
       WHERE
         persistence_id = ?
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
