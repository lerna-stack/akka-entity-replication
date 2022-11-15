package lerna.akka.entityreplication.rollback

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex

import java.time.Instant

private final case class RaftShardRollbackParameters(
    typeName: TypeName,
    shardId: NormalizedShardId,
    allMemberIndices: Set[MemberIndex],
    leaderMemberIndex: MemberIndex,
    toTimestamp: Instant,
) {
  require(
    allMemberIndices.contains(leaderMemberIndex),
    s"allMemberIndices [$allMemberIndices] should contain the leader member index [$leaderMemberIndex]",
  )
}
