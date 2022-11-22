package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor

private[rollback] final case class CommitLogStoreActorId(
    typeName: TypeName,
    shardId: NormalizedShardId,
) {

  lazy val persistenceId: String =
    CommitLogStoreActor.persistenceId(typeName, shardId.raw)

}
