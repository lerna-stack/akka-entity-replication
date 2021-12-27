package lerna.akka.entityreplication.typed.internal.testkit

import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, SerializationTestKit }
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntryIndex, NoOp, ReplicatedLog, Term }
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol
import lerna.akka.entityreplication.typed.ClusterReplication.ShardCommand
import lerna.akka.entityreplication.typed.{ ReplicatedEntityContext, ReplicatedEntityTypeKey }
import lerna.akka.entityreplication.typed.testkit.ReplicatedEntityBehaviorTestKit

import scala.annotation.tailrec
import scala.util.control.NonFatal

private[entityreplication] class ReplicatedEntityBehaviorTestKitImpl[Command, Event, State](
    actorTestKit: ActorTestKit,
    entityTypeKey: ReplicatedEntityTypeKey[Command],
    entityId: String,
    behavior: ReplicatedEntityContext[Command] => Behavior[Command],
) extends ReplicatedEntityBehaviorTestKit[Command, Event, State] {

  private[this] val normalizedEntityId   = NormalizedEntityId.from(entityId)
  private[this] val serializationTestKit = new SerializationTestKit(actorTestKit.system)
  private[this] val shardProbe           = actorTestKit.createTestProbe[ShardCommand]()
  private[this] val snapshotProbe        = actorTestKit.createTestProbe[RaftProtocol.Snapshot]()
  private val shardSnapshotStoreProbe    = actorTestKit.createTestProbe[SnapshotProtocol.Command]()
  private[this] val entityContext = new ReplicatedEntityContext[Command](
    entityTypeKey,
    entityId = entityId,
    shard = shardProbe.ref,
  )
  private[this] val term                                   = Term(1)
  private[this] var replicatedLog: ReplicatedLog           = ReplicatedLog()
  private[this] var replicatedEntityRef: ActorRef[Command] = spawnAndRecoverReplicatedEntityRef()

  private[this] implicit class ReplicatedEntityRefOperations(replicatedEntityRef: ActorRef[Command]) {
    def asEntity: ActorRef[RaftProtocol.EntityCommand] =
      replicatedEntityRef.asInstanceOf[ActorRef[RaftProtocol.EntityCommand]]
  }

  override def runCommand(command: Command): ReplicatedEntityBehaviorTestKit.CommandResult[Command, Event, State] = {
    verifyPreRunCommand(command)
    replicatedEntityRef ! command

    val newEvent = handleReplicatedEvent()
    val newState = state

    verifyPostRunCommand(newEvent, newState, reply = None)
    CommandResultImpl(command, newEvent, newState, replyOption = None)
  }

  override def runCommand[Reply](
      creator: ActorRef[Reply] => Command,
  ): ReplicatedEntityBehaviorTestKit.CommandResultWithReply[Command, Event, State, Reply] = {
    val replyProbe = actorTestKit.createTestProbe[Reply]()
    val command    = creator(replyProbe.ref)
    verifyPreRunCommand(command)
    replicatedEntityRef ! command

    val newEvent = handleReplicatedEvent()
    val newState = state
    val reply =
      try {
        Option(replyProbe.receiveMessage())
      } catch {
        case NonFatal(_) => None
      } finally {
        replyProbe.stop()
      }

    verifyPostRunCommand(newEvent, newState, reply)
    CommandResultImpl(command, newEvent, newState, reply)
  }

  override def state: State = {
    val metadata = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, replicatedLog.lastLogIndex)
    replicatedEntityRef.asEntity ! RaftProtocol.TakeSnapshot(metadata, snapshotProbe.ref.toClassic)
    val snapshot = snapshotProbe.receiveMessage()
    snapshot.state.underlying.asInstanceOf[State]
  }

  override def restart(): ReplicatedEntityBehaviorTestKit.RestartResult[State] = {
    actorTestKit.stop(replicatedEntityRef)
    replicatedEntityRef = spawnAndRecoverReplicatedEntityRef()
    try {
      RestartResultImpl(state)
    } catch {
      case NonFatal(ex) =>
        throw new IllegalStateException("Restart failed. Maybe exception from event handler. See logs.", ex)
    }
  }

  override def clear(): Unit = {
    replicatedLog = ReplicatedLog()
    restart()
  }

  private[this] def spawnAndRecoverReplicatedEntityRef(): ActorRef[Command] = {
    val ref = actorTestKit.spawn(behavior(entityContext))
    ref.asEntity ! RaftProtocol.Activate(shardSnapshotStoreProbe.ref.toClassic, recoveryIndex = LogEntryIndex.initial())
    val fetchSnapshot = shardSnapshotStoreProbe.expectMessageType[SnapshotProtocol.FetchSnapshot]
    fetchSnapshot.replyTo ! SnapshotProtocol.SnapshotNotFound(fetchSnapshot.entityId)
    val fetchEvents = shardProbe.expectMessageType[FetchEntityEvents]
    fetchEvents.replyTo ! FetchEntityEventsResponse(replicatedLog.entries)
    ref
  }

  private[this] def handleReplicatedEvent(): Option[Event] = {
    @tailrec
    def findReplicateCommand(): Option[RaftProtocol.Replicate] = {
      val shardCommand: Option[ShardCommand] =
        try {
          Option(shardProbe.receiveMessage())
        } catch {
          case NonFatal(_) => None // It wasn't replicated until the timeout
        }
      shardCommand match {
        case Some(r: RaftProtocol.Replicate)      => Option(r)
        case Some(r: ReplicationRegion.Passivate) => findReplicateCommand() // retry
        case Some(m)                              => throw new IllegalStateException(s"Shard received unexpected message [$m]")
        case None                                 => None
      }
    }
    val replicateCommand = findReplicateCommand()
    replicateCommand.foreach { replicate =>
      // side effects
      replicatedLog = replicatedLog.append(EntityEvent(Option(normalizedEntityId), replicate.event), term)
      replicate.replyTo ! RaftProtocol.ReplicationSucceeded(
        replicate.event,
        replicatedLog.lastLogIndex,
        replicate.instanceId,
      )
    }
    replicateCommand.map(_.event).flatMap {
      case NoOp  => None // NoOp is not user-defined event
      case event => Option(event.asInstanceOf[Event])
    }
  }

  private[this] def verifyPreRunCommand(command: Command): Unit = {
    verifySerialization(command, "Command")
  }

  private[this] def verifyPostRunCommand[Reply](
      newEvent: Option[Event],
      newState: State,
      reply: Option[Reply],
  ): Unit = {
    newEvent.foreach(verifySerialization(_, "Event"))
    verifySerialization(newState, "State")
    reply.foreach(verifySerialization(_, "Reply"))
  }

  private[this] def verifySerialization(obj: Any, objCategory: String): Unit = {
    try {
      serializationTestKit.verifySerialization(obj, assertEquality = true)
    } catch {
      case NonFatal(ex) =>
        throw new AssertionError(s"$objCategory [$obj] isn't serializable", ex)
    }
  }
}
