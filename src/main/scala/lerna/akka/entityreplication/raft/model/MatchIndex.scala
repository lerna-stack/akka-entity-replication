package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.raft.routing.MemberIndex

case class MatchIndex(indexes: Map[MemberIndex, LogEntryIndex] = Map()) {

  def apply(follower: MemberIndex): LogEntryIndex = {
    indexes.getOrElse(follower, LogEntryIndex.initial())
  }

  def update(follower: MemberIndex, index: LogEntryIndex): MatchIndex = {
    copy(indexes + (follower -> index))
  }

  def countMatch(predicate: LogEntryIndex => Boolean): Int = {
    indexes.values.count(predicate)
  }
}
