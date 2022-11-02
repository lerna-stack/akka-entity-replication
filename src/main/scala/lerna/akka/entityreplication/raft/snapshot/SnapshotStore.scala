package lerna.akka.entityreplication.raft.snapshot

import akka.actor.{ ActorLogging, ActorRef, Props, ReceiveTimeout }
import akka.persistence
import akka.persistence.PersistentActor
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.util.ActorIds

private[entityreplication] object SnapshotStore {

  def props(
      typeName: TypeName,
      entityId: NormalizedEntityId,
      settings: RaftSettings,
      selfMemberIndex: MemberIndex,
  ): Props =
    Props(new SnapshotStore(typeName, entityId, settings, selfMemberIndex))

  /** Returns a persistence ID of SnapshotStore */
  def persistenceId(typeName: TypeName, entityId: NormalizedEntityId, selfMemberIndex: MemberIndex): String =
    ActorIds.persistenceId("SnapshotStore", typeName.underlying, entityId.underlying, selfMemberIndex.role)

}

private[entityreplication] class SnapshotStore(
    typeName: TypeName,
    entityId: NormalizedEntityId,
    settings: RaftSettings,
    selfMemberIndex: MemberIndex,
) extends PersistentActor
    with ActorLogging {
  import SnapshotProtocol._

  private var replyRefCache: Option[ActorRef] = None

  override def persistenceId: String =
    SnapshotStore.persistenceId(typeName, entityId, selfMemberIndex)

  override def journalPluginId: String = settings.journalPluginId

  override def snapshotPluginId: String = settings.snapshotStorePluginId

  context.setReceiveTimeout(settings.compactionSnapshotCacheTimeToLive)

  override def receiveRecover: Receive = {
    case snapshot: EntitySnapshot => context.become(hasSnapshot(snapshot))
    case akka.persistence.SnapshotOffer(_, s: EntitySnapshot) =>
      context.become(hasSnapshot(s))
  }

  override def receiveCommand: Receive = hasNoSnapshot

  override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Saving snapshot failed - {}: {}",
        cause.getClass.getCanonicalName,
        cause.getMessage,
      )
    }
    super.onPersistFailure(cause, event, seqNr)
    replyRefCache.fold(
      if (log.isWarningEnabled) log.warning("missing reply reference - {}", cause.getClass.getCanonicalName),
    )(
      _ ! SaveSnapshotFailure(event.asInstanceOf[EntitySnapshot].metadata),
    )
  }

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
    if (prevSnapshot.exists(_.metadata == command.snapshot.metadata)) {
      // reduce IO: don't save if same as cached snapshot
      command.replyTo ! SaveSnapshotSuccess(command.snapshot.metadata)
    } else {
      replyRefCache = Some(command.replyTo)
      persistAsync(command.snapshot) { _ =>
        command.replyTo ! SaveSnapshotSuccess(command.snapshot.metadata)
        if (lastSequenceNr % settings.snapshotStoreSnapshotInterval == 0 && lastSequenceNr != 0) {
          saveSnapshot(command.snapshot)
        }
        context.become(hasSnapshot(command.snapshot))
      }
      context.become(savingSnapshot(command.replyTo, command.snapshot, prevSnapshot))
    }
  }

  def savingSnapshot(replyTo: ActorRef, snapshot: EntitySnapshot, prevSnapshot: Option[EntitySnapshot]): Receive = {
    case command: Command =>
      command match {
        case cmd: SaveSnapshot =>
          if (log.isWarningEnabled)
            log.warning(
              "Saving snapshot for an entity ({}) currently. Consider to increase log-size-threshold or log-size-check-interval.",
              cmd.entityId,
            )
        case FetchSnapshot(_, replyTo) =>
          prevSnapshot match {
            case Some(prevSnapshot) =>
              replyTo ! SnapshotProtocol.SnapshotFound(prevSnapshot)
            case None =>
              replyTo ! SnapshotProtocol.SnapshotNotFound(entityId)
          }
      }
  }

  override def unhandled(message: Any): Unit =
    message match {
      case ReceiveTimeout =>
        context.stop(self)
      case _: persistence.SaveSnapshotSuccess =>
        if (log.isDebugEnabled) {
          log.debug("Saving EntitySnapshot as a snapshot succeeded.")
        }
      case failure: persistence.SaveSnapshotFailure =>
        if (log.isDebugEnabled) {
          log.debug(
            "Saving EntitySnapshot as a snapshot failed. - {}: {}",
            failure.cause.getClass.getCanonicalName,
            failure.cause.getMessage,
          )
        }
      case _ =>
        super.unhandled(message)
    }
}
