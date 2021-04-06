package lerna.akka.entityreplication.raft.model

private[entityreplication] object SnapshotStatus {
  def empty: SnapshotStatus = SnapshotStatus(Term.initial(), LogEntryIndex.initial())
}

private[entityreplication] final case class SnapshotStatus(snapshotLastTerm: Term, snapshotLastLogIndex: LogEntryIndex)
