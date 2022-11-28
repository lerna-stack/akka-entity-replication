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

  /** [[RaftActor]] (especially [[Leader]]) will replicate the given `event` when it receives this message.
    *
    * This message is used for two purposes:
    *  - Used for internal (by RaftActor itself):
    *    - [[Replicate.ReplicateForInternal]] is for this use case.
    *    - RaftActor sends this message to itself when it becomes the leader.
    *  - Used for an entity:
    *    - [[Replicate.ReplicateForEntity]] is for this use case.
    *    - An entity sends this message to its RaftActor.
    *
    * RaftActor will reply with [[ReplicationResponse]] to this message.
    */
  sealed trait Replicate extends RaftActorCommand {
    def event: Any
    def replyTo: ActorRef
    def entityId: Option[NormalizedEntityId]
    def instanceId: Option[EntityInstanceId]
    def entityLastAppliedIndex: Option[LogEntryIndex]
    def originSender: Option[ActorRef]
  }
  object Replicate {

    /** used for internal use (RaftActor sends this message to itself) */
    final case class ReplicateForInternal(
        event: Any,
        replyTo: ActorRef,
    ) extends Replicate {
      override def entityId: Option[NormalizedEntityId]          = None
      override def instanceId: Option[EntityInstanceId]          = None
      override def entityLastAppliedIndex: Option[LogEntryIndex] = None
      override def originSender: Option[ActorRef]                = None
    }

    /** used for an entity (An entity sends this message to its RaftActor) */
    final case class ReplicateForEntity(
        event: Any,
        replyTo: ActorRef,
        _entityId: NormalizedEntityId,
        _instanceId: EntityInstanceId,
        _entityLastAppliedIndex: LogEntryIndex,
        _originSender: ActorRef,
    ) extends Replicate {
      override def entityId: Option[NormalizedEntityId]          = Option(_entityId)
      override def instanceId: Option[EntityInstanceId]          = Option(_instanceId)
      override def entityLastAppliedIndex: Option[LogEntryIndex] = Option(_entityLastAppliedIndex)
      override def originSender: Option[ActorRef]                = Option(_originSender)
    }

    /** Creates a [[Replicate]] message for an entity */
    def apply(
        event: Any,
        replyTo: ActorRef,
        entityId: NormalizedEntityId,
        instanceId: EntityInstanceId,
        entityLastAppliedIndex: LogEntryIndex,
        originSender: ActorRef,
    ): Replicate = {
      ReplicateForEntity(event, replyTo, entityId, instanceId, entityLastAppliedIndex, originSender)
    }

    /** Creates a [[Replicate]] message for internal use */
    def internal(event: Any, replyTo: ActorRef): Replicate = {
      ReplicateForInternal(event, replyTo)
    }

  }

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
