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
import lerna.akka.entityreplication.typed.ClusterReplication.ShardCommand

private[entityreplication] object RaftProtocol {

  sealed trait RaftActorCommand                                                   extends ShardCommand
  final case class Command(command: Any)                                          extends RaftActorCommand with ClusterReplicationSerializable
  final case class ForwardedCommand(command: Command)                             extends RaftActorCommand with ClusterReplicationSerializable
  final case class Snapshot(metadata: EntitySnapshotMetadata, state: EntityState) extends RaftActorCommand
  final case class EntityTerminated(entityId: NormalizedEntityId)                 extends RaftActorCommand

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

  final case class Replicate(
      event: Any,
      replyTo: ActorRef,
      entityId: Option[NormalizedEntityId],
      instanceId: Option[EntityInstanceId],
      originSender: Option[ActorRef],
  ) extends RaftActorCommand

  sealed trait EntityCommand

  final case class Activate(shardSnapshotStore: ActorRef, recoveryIndex: LogEntryIndex)   extends EntityCommand
  final case class ApplySnapshot(entitySnapshot: Option[EntitySnapshot])                  extends EntityCommand
  final case class RecoveryState(events: Seq[LogEntry], snapshot: Option[EntitySnapshot]) extends EntityCommand
  final case class ProcessCommand(command: Any)                                           extends EntityCommand
  final case class Replica(logEntry: LogEntry)                                            extends EntityCommand
  final case class TakeSnapshot(metadata: EntitySnapshotMetadata, replyTo: ActorRef)      extends EntityCommand
  final case object RecoveryTimeout                                                       extends EntityCommand

  sealed trait ReplicationResponse extends EntityCommand
  final case class ReplicationSucceeded(event: Any, logEntryIndex: LogEntryIndex, instanceId: Option[EntityInstanceId])
      extends ReplicationResponse
  final case object ReplicationFailed extends ReplicationResponse

  final case class EntityRecoveryTimeoutException(entityPath: ActorPath) extends RuntimeException
}
