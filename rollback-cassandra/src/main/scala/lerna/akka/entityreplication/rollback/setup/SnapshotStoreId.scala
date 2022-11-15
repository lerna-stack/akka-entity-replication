package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotStore

private[rollback] final case class SnapshotStoreId(
    typeName: TypeName,
    memberIndex: MemberIndex,
    entityId: NormalizedEntityId,
) {

  lazy val persistenceId: String =
    SnapshotStore.persistenceId(typeName, entityId, memberIndex)

}
