package lerna.akka.entityreplication.raft.snapshot

import akka.actor.{ ActorLogging, ActorRef, Props, ReceiveTimeout }
import akka.persistence
import akka.persistence.PersistentActor
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.util.ActorIds

object SnapshotStore {

  def props(
      typeName: TypeName,
      entityId: NormalizedEntityId,
      settings: RaftSettings,
      selfMemberIndex: MemberIndex,
  ): Props =
    Props(new SnapshotStore(typeName, entityId, settings, selfMemberIndex))
}

class SnapshotStore(
    typeName: TypeName,
    entityId: NormalizedEntityId,
    settings: RaftSettings,
    selfMemberIndex: MemberIndex,
) extends PersistentActor
    with ActorLogging {
  import SnapshotProtocol._

  override def persistenceId: String =
    ActorIds.persistenceId("SnapshotStore", typeName.underlying, entityId.underlying, selfMemberIndex.role)

  override def journalPluginId: String = settings.journalPluginId

  override def snapshotPluginId: String = settings.snapshotStorePluginId

  context.setReceiveTimeout(settings.compactionSnapshotCacheTimeToLive)

  override def receiveRecover: Receive = {
    case akka.persistence.SnapshotOffer(_, s: EntitySnapshot) =>
      context.become(hasSnapshot(s))
  }

  override def receiveCommand: Receive = hasNoSnapshot

  def hasNoSnapshot: Receive = {
    case command: Command =>
      command match {
        case cmd: SaveSnapshot =>
          handleSaveSnapshot(cmd, prevSnapshot = None)
        case FetchSnapshot(_, replyTo) =>
          replyTo ! SnapshotProtocol.SnapshotNotFound(entityId)
      }
  }

  def hasSnapshot(snapshot: EntitySnapshot): Receive = {
    case command: Command =>
      command match {
        case cmd: SaveSnapshot =>
          handleSaveSnapshot(cmd, Option(snapshot))
        case FetchSnapshot(_, replyTo) =>
          replyTo ! SnapshotProtocol.SnapshotFound(snapshot)
      }
  }

  def handleSaveSnapshot(command: SaveSnapshot, prevSnapshot: Option[EntitySnapshot]): Unit = {
    saveSnapshot(command.snapshot)
    context.become(savingSnapshot(command.replyTo, command.snapshot, prevSnapshot))
  }

  def savingSnapshot(replyTo: ActorRef, snapshot: EntitySnapshot, prevSnapshot: Option[EntitySnapshot]): Receive = {
    case command: Command =>
      command match {
        case cmd: SaveSnapshot =>
          log.warning(
            s"Saving snapshot for an entity (${cmd.entityId}) currently. Consider to increase log-size-threshold or log-size-check-interval.",
          )
        case FetchSnapshot(_, replyTo) =>
          prevSnapshot.foreach { s =>
            replyTo ! SnapshotProtocol.SnapshotFound(s)
          }
      }
    case _: persistence.SaveSnapshotSuccess =>
      replyTo ! SaveSnapshotSuccess(snapshot.metadata)
      context.become(hasSnapshot(snapshot))
    case _: persistence.SaveSnapshotFailure =>
      replyTo ! SaveSnapshotFailure(snapshot.metadata)
  }

  override def unhandled(message: Any): Unit =
    message match {
      case ReceiveTimeout =>
        context.stop(self)
      case _ =>
        super.unhandled(message)
    }
}
