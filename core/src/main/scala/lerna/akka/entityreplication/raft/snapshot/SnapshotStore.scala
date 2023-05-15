package lerna.akka.entityreplication.raft.snapshot

import akka.actor.{ ActorLogging, ActorRef, Props, ReceiveTimeout }
import akka.persistence
import akka.persistence.{ PersistentActor, SnapshotSelectionCriteria }
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

  override def preStart(): Unit = {
    if (log.isDebugEnabled) {
      log.debug(
        "Starting SnapshotStore (typeName=[{}], entityId=[{}], memberIndex=[{}]), which will stop when it is inactive for snapshot-cache-time-to-live period [{}] ms",
        typeName,
        entityId.raw,
        selfMemberIndex,
        settings.compactionSnapshotCacheTimeToLive.toMillis,
      )
    }
    super.preStart()
    context.setReceiveTimeout(settings.compactionSnapshotCacheTimeToLive)
  }

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
      if (log.isDebugEnabled) {
        log.debug(
          "Saving EntitySnapshot: entityId=[{}], logEntryIndex=[{}], stateType=[{}]",
          command.snapshot.metadata.entityId.raw,
          command.snapshot.metadata.logEntryIndex,
          command.snapshot.state.underlying.getClass.getName,
        )
      }
      replyRefCache = Option(command.replyTo)
      persistAsync(command.snapshot) { _ =>
        if (log.isDebugEnabled) {
          log.debug(
            "Saved EntitySnapshot: entityId=[{}], logEntryIndex=[{}], stateType=[{}]",
            command.snapshot.metadata.entityId.raw,
            command.snapshot.metadata.logEntryIndex,
            command.snapshot.state.underlying.getClass.getName,
          )
        }
        command.replyTo ! SaveSnapshotSuccess(command.snapshot.metadata)
        if (lastSequenceNr % settings.snapshotStoreSnapshotEvery == 0 && lastSequenceNr != 0) {
          if (log.isDebugEnabled) {
            log.debug(
              "Saving EntitySnapshot as a snapshot: entityId=[{}], logEntryIndex=[{}], stateType=[{}]",
              command.snapshot.metadata.entityId.raw,
              command.snapshot.metadata.logEntryIndex,
              command.snapshot.state.underlying.getClass.getName,
            )
          }
          saveSnapshot(command.snapshot)
        }
        context.become(hasSnapshot(command.snapshot))
      }
      context.become(savingSnapshot(prevSnapshot))
    }
  }

  def savingSnapshot(prevSnapshot: Option[EntitySnapshot]): Receive = {
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
        if (log.isDebugEnabled) {
          log.debug(
            "Stopping SnapshotStore (typeName=[{}], entityId=[{}], memberIndex=[{}]) since it is inactive",
            typeName,
            entityId.raw,
            selfMemberIndex,
          )
        }
        context.stop(self)

      case success: persistence.SaveSnapshotSuccess =>
        handleSaveSnapshotSuccess(success)
      case failure: persistence.SaveSnapshotFailure =>
        handleSaveSnapshotFailure(failure)
      case success: persistence.DeleteMessagesSuccess =>
        handleDeleteMessagesSuccess(success)
      case failure: persistence.DeleteMessagesFailure =>
        handleDeleteMessagesFailure(failure)
      case success: persistence.DeleteSnapshotsSuccess =>
        handleDeleteSnapshotsSuccess(success)
      case failure: persistence.DeleteSnapshotsFailure =>
        handleDeleteSnapshotsFailure(failure)

      case _ =>
        super.unhandled(message)
    }

  private def handleSaveSnapshotSuccess(success: persistence.SaveSnapshotSuccess): Unit = {
    if (log.isDebugEnabled) {
      log.debug("Succeeded to saveSnapshot given metadata [{}]", success.metadata)
    }
    val deleteBeforeRelativeSequenceNr = settings.snapshotStoreDeleteBeforeRelativeSequenceNr
    if (settings.snapshotStoreDeleteOldEvents) {
      val deleteEventsToSequenceNr =
        math.max(0, success.metadata.sequenceNr - deleteBeforeRelativeSequenceNr)
      if (deleteEventsToSequenceNr > 0) {
        if (log.isDebugEnabled) {
          log.debug("Deleting events up to sequenceNr [{}]", deleteEventsToSequenceNr)
        }
        deleteMessages(deleteEventsToSequenceNr)
      }
    }
    if (settings.snapshotStoreDeleteOldSnapshots) {
      // Since this actor will use the snapshot with sequence number `metadata.sequenceNr` for recovery, it can delete
      // snapshots with sequence numbers less than `metadata.sequenceNr`.
      val deleteSnapshotsToSequenceNr =
        math.max(0, success.metadata.sequenceNr - 1 - deleteBeforeRelativeSequenceNr)
      if (deleteSnapshotsToSequenceNr > 0) {
        val deletionCriteria =
          SnapshotSelectionCriteria(minSequenceNr = 0, maxSequenceNr = deleteSnapshotsToSequenceNr)
        if (log.isDebugEnabled) {
          log.debug("Deleting snapshots matching criteria [{}]", deletionCriteria)
        }
        deleteSnapshots(deletionCriteria)
      }
    }
  }

  private def handleSaveSnapshotFailure(failure: persistence.SaveSnapshotFailure): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Failed to saveSnapshot given metadata [{}] due to: [{}: {}]",
        failure.metadata,
        failure.cause.getClass.getCanonicalName,
        failure.cause.getMessage,
      )
    }
  }

  private def handleDeleteMessagesSuccess(success: persistence.DeleteMessagesSuccess): Unit = {
    if (log.isDebugEnabled) {
      log.debug("Succeeded to deleteMessages up to sequenceNr [{}]", success.toSequenceNr)
    }
  }

  private def handleDeleteMessagesFailure(failure: persistence.DeleteMessagesFailure): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Failed to deleteMessages given toSequenceNr [{}] due to: [{}: {}]",
        failure.toSequenceNr,
        failure.cause.getClass.getCanonicalName,
        failure.cause.getMessage,
      )
    }
  }

  private def handleDeleteSnapshotsSuccess(success: persistence.DeleteSnapshotsSuccess): Unit = {
    if (log.isDebugEnabled) {
      log.debug("Succeeded to deleteSnapshots given criteria [{}]", success.criteria)
    }
  }

  private def handleDeleteSnapshotsFailure(failure: persistence.DeleteSnapshotsFailure): Unit = {
    if (log.isWarningEnabled) {
      log.warning(
        "Failed to deleteSnapshots given criteria [{}] due to: [{}: {}]",
        failure.criteria,
        failure.cause.getClass.getCanonicalName,
        failure.cause.getMessage,
      )
    }
  }

}
