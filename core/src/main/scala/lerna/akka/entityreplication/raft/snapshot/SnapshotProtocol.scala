package lerna.akka.entityreplication.raft.snapshot

import akka.actor.ActorRef
import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model.LogEntryIndex

private[entityreplication] object SnapshotProtocol {

  sealed trait Command {
    def entityId: NormalizedEntityId
  }
  final case class SaveSnapshot(snapshot: EntitySnapshot, replyTo: ActorRef) extends Command {
    override def entityId: NormalizedEntityId = snapshot.metadata.entityId
  }
  final case class FetchSnapshot(entityId: NormalizedEntityId, replyTo: ActorRef) extends Command

  sealed trait Response {
    def metadata: EntitySnapshotMetadata
  }
  sealed trait SaveSnapshotResponse                                      extends Response
  final case class SaveSnapshotSuccess(metadata: EntitySnapshotMetadata) extends SaveSnapshotResponse
  final case class SaveSnapshotFailure(metadata: EntitySnapshotMetadata) extends SaveSnapshotResponse
  sealed trait FetchSnapshotResponse
  final case class SnapshotFound(snapshot: EntitySnapshot)        extends FetchSnapshotResponse
  final case class SnapshotNotFound(entityId: NormalizedEntityId) extends FetchSnapshotResponse

  final case class EntityState(underlying: Any)
  final case class EntitySnapshotMetadata(
      entityId: NormalizedEntityId,
      logEntryIndex: LogEntryIndex,
  )
  final case class EntitySnapshot(
      metadata: EntitySnapshotMetadata,
      state: EntityState,
  ) extends ClusterReplicationSerializable
}
