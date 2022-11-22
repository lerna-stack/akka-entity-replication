package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.rollback.SequenceNr

private[rollback] final case class RaftActorRollbackSetup(
    id: RaftActorId,
    to: Option[SequenceNr],
)
