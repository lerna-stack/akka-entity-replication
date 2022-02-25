package lerna.akka.entityreplication.raft.model

private[entityreplication] object SnapshotStatus {
  def empty: SnapshotStatus =
    SnapshotStatus(
      snapshotLastTerm = Term.initial(),
      snapshotLastLogIndex = LogEntryIndex.initial(),
      targetSnapshotLastTerm = Term.initial(),
      targetSnapshotLastLogIndex = LogEntryIndex.initial(),
    )
}

/**
  * Tracks status of snapshots.
  *
  * @param snapshotLastTerm Maximum [[Term]] in all persisted snapshots
  * @param snapshotLastLogIndex Maximum [[LogEntryIndex]] in all persisted snapshots
  * @param targetSnapshotLastTerm Maximum [[Term]] of snapshots that might be persisted
  * @param targetSnapshotLastLogIndex Maximum [[LogEntryIndex]] of snapshots that might be persisted
  */
private[entityreplication] final case class SnapshotStatus(
    snapshotLastTerm: Term,
    snapshotLastLogIndex: LogEntryIndex,
    targetSnapshotLastTerm: Term,
    targetSnapshotLastLogIndex: LogEntryIndex,
) {

  require(
    snapshotLastTerm <= targetSnapshotLastTerm && snapshotLastLogIndex <= targetSnapshotLastLogIndex,
    Seq(
      s"(snapshotLastTerm[$snapshotLastTerm], snapshotLastLogIndex[$snapshotLastLogIndex])",
      "must not exceed",
      s"(targetSnapshotLastTerm[$targetSnapshotLastTerm], targetSnapshotLastLogIndex[$targetSnapshotLastLogIndex]",
    ).mkString(" "),
  )

  def updateSnapshotsCompletely(snapshotLastTerm: Term, snapshotLastLogIndex: LogEntryIndex): SnapshotStatus =
    copy(
      snapshotLastTerm = snapshotLastTerm,
      snapshotLastLogIndex = snapshotLastLogIndex,
      targetSnapshotLastTerm = snapshotLastTerm,
      targetSnapshotLastLogIndex = snapshotLastLogIndex,
    )

  def startSnapshotSync(snapshotLastTerm: Term, snapshotLastLogIndex: LogEntryIndex): SnapshotStatus =
    copy(
      targetSnapshotLastTerm = snapshotLastTerm,
      targetSnapshotLastLogIndex = snapshotLastLogIndex,
    )

  def completeSnapshotSync(snapshotLastTerm: Term, snapshotLastLogIndex: LogEntryIndex): SnapshotStatus =
    copy(
      snapshotLastTerm = snapshotLastTerm,
      snapshotLastLogIndex = snapshotLastLogIndex,
    )

  /**
    * `true` means snapshot-synchronization has started but it has not completed yet.
    */
  def isDirty: Boolean =
    snapshotLastTerm != targetSnapshotLastTerm && snapshotLastLogIndex != targetSnapshotLastLogIndex
}
