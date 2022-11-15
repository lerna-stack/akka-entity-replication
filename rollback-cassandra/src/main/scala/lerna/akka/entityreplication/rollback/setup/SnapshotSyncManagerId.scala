package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager

private[rollback] final case class SnapshotSyncManagerId(
    typeName: TypeName,
    shardId: NormalizedShardId,
    sourceMemberIndex: MemberIndex,
    destinationMemberIndex: MemberIndex,
) {

  lazy val persistenceId: String =
    SnapshotSyncManager.persistenceId(typeName, sourceMemberIndex, destinationMemberIndex, shardId)

}
