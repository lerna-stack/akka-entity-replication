package lerna.akka.entityreplication.raft.model

object SnapshotStatus {
  def empty: SnapshotStatus = SnapshotStatus(Term.initial(), LogEntryIndex.initial())
}

final case class SnapshotStatus(snapshotLastTerm: Term, snapshotLastLogIndex: LogEntryIndex)
