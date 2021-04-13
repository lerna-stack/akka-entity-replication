package lerna.akka.entityreplication.raft

import akka.actor.{ ActorPath, ActorRef }
import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId }
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.{
  EntitySnapshot,
  EntitySnapshotMetadata,
  EntityState,
}

private[entityreplication] object RaftProtocol {

  final case class RequestRecovery(entityId: NormalizedEntityId)
  final case class RecoveryState(events: Seq[LogEntry], snapshot: Option[EntitySnapshot])

  case class Command(command: Any)              extends ClusterReplicationSerializable
  case class ForwardedCommand(command: Command) extends ClusterReplicationSerializable
  case class Replica(logEntry: LogEntry)

  object Replicate {
    def apply(
        event: Any,
        replyTo: ActorRef,
        entityId: NormalizedEntityId,
        instanceId: EntityInstanceId,
        originSender: ActorRef,
    ): Replicate = {
      Replicate(event, replyTo, Option(entityId), Option(instanceId), Option(originSender))
    }

    def internal(event: Any, replyTo: ActorRef): Replicate = {
      Replicate(event, replyTo, None, None, None)
    }
  }

  case class Replicate(
      event: Any,
      replyTo: ActorRef,
      entityId: Option[NormalizedEntityId],
      instanceId: Option[EntityInstanceId],
      originSender: Option[ActorRef],
  )

  sealed trait ReplicationResponse

  case class ReplicationSucceeded(event: Any, logEntryIndex: LogEntryIndex, instanceId: Option[EntityInstanceId])
      extends ReplicationResponse

  final case class TakeSnapshot(metadata: EntitySnapshotMetadata, replyTo: ActorRef)
  final case class Snapshot(metadata: EntitySnapshotMetadata, state: EntityState)

  final case object RecoveryTimeout

  final case class EntityRecoveryTimeoutException(entityPath: ActorPath) extends RuntimeException
}
