package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.raft.routing.MemberIndex

case class NextIndex(leaderLog: ReplicatedLog, indexes: Map[MemberIndex, LogEntryIndex] = Map()) {

  val initialLogIndex: LogEntryIndex = leaderLog.lastOption.map(_.index).getOrElse(LogEntryIndex.initial()).next()

  def apply(followerMemberIndex: MemberIndex): LogEntryIndex = {
    indexes.getOrElse(followerMemberIndex, initialLogIndex)
  }

  def update(followerMemberIndex: MemberIndex, index: LogEntryIndex): NextIndex = {
    copy(indexes = indexes + (followerMemberIndex -> index))
  }
}
