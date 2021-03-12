package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.model.NormalizedEntityId

object SnapshottingStatus {
  def empty: SnapshottingStatus =
    SnapshottingStatus(
      snapshotLastLogTerm = Term.initial(),
      snapshotLastLogIndex = LogEntryIndex.initial(),
      inProgressEntities = Set(),
      completedEntities = Set(),
    )
}

case class SnapshottingStatus(
    snapshotLastLogTerm: Term,
    snapshotLastLogIndex: LogEntryIndex,
    inProgressEntities: Set[NormalizedEntityId],
    completedEntities: Set[NormalizedEntityId],
) {

  def isInProgress: Boolean = inProgressEntities.nonEmpty

  def isCompleted: Boolean = inProgressEntities.isEmpty

  def recordSnapshottingComplete(
      snapshotLastLogIndex: LogEntryIndex,
      entityId: NormalizedEntityId,
  ): SnapshottingStatus = {
    require(
      snapshotLastLogIndex == this.snapshotLastLogIndex,
      s"snapshotLastLogIndexes should be same (current: ${this.snapshotLastLogIndex}, got: ${snapshotLastLogIndex})",
    )
    copy(inProgressEntities = inProgressEntities - entityId, completedEntities = completedEntities + entityId)
  }
}
