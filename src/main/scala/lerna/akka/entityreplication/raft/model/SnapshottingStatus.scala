package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.model.NormalizedEntityId

object SnapshottingStatus {
  def empty: SnapshottingStatus =
    SnapshottingStatus(snapshotLastLogIndex = LogEntryIndex.initial(), inProgressEntities = Set())
}

case class SnapshottingStatus(snapshotLastLogIndex: LogEntryIndex, inProgressEntities: Set[NormalizedEntityId]) {

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
    copy(inProgressEntities = inProgressEntities - entityId)
  }
}
