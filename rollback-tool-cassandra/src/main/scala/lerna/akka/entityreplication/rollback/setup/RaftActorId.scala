package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.util.ActorIds

private[rollback] final case class RaftActorId(
    typeName: TypeName,
    shardId: NormalizedShardId,
    memberIndex: MemberIndex,
) {

  lazy val persistenceId: String =
    ActorIds.persistenceId("raft", typeName.underlying, shardId.underlying, memberIndex.role)

}
