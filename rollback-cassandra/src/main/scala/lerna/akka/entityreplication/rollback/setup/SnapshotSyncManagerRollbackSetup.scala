package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.rollback.SequenceNr

private[rollback] final case class SnapshotSyncManagerRollbackSetup(
    id: SnapshotSyncManagerId,
    to: Option[SequenceNr],
)
