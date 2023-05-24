package lerna.akka.entityreplication.rollback.testkit

import akka.NotUsed
import akka.actor.{ ActorLogging, ActorRef, Props }
import akka.persistence._
import akka.persistence.journal.Tagged
import lerna.akka.entityreplication.rollback.JsonSerializable

object TestPersistentActor {

  def props(persistenceId: String): Props = {
    Props(new TestPersistentActor(persistenceId))
  }

  sealed trait Command                                                extends JsonSerializable
  final case class GetRecoveryTracking(replyTo: ActorRef)             extends Command
  final case class PersistEvent(replyTo: ActorRef)                    extends Command
  final case class PersistTaggedEvent(tag: String, replyTo: ActorRef) extends Command
  final case class PersistEventsAtomically(numOfEvents: Int, replyTo: ActorRef) extends Command {
    require(numOfEvents >= 1)
  }
  final case class SaveSnapshot(replyTo: ActorRef)                          extends Command
  final case class DeleteEventsTo(toSequenceNr: Long, replyTo: ActorRef)    extends Command
  final case class DeleteSnapshotsTo(toSequenceNr: Long, replyTo: ActorRef) extends Command

  final case class Event(sequenceNr: Long)    extends JsonSerializable
  final case class Snapshot(sequenceNr: Long) extends JsonSerializable

  sealed trait Reply                     extends JsonSerializable
  final case class Ack(sequenceNr: Long) extends Reply
  final case class RecoveryTracking(offeredSnapshot: Option[Snapshot], replayedEvents: Vector[Event]) extends Reply {
    def withOfferedSnapshot(snapshot: Snapshot): RecoveryTracking =
      copy(offeredSnapshot = Some(snapshot))
    def withReplayedEvent(event: Event): RecoveryTracking =
      copy(replayedEvents = replayedEvents :+ event)
  }

}

final class TestPersistentActor private (val persistenceId: String) extends PersistentActor with ActorLogging {
  import TestPersistentActor._

  var recoveryTracking: RecoveryTracking           = RecoveryTracking(None, Vector.empty)
  var lastSaveSnapshotReplyTo: Option[ActorRef]    = None
  var lastDeleteEventsReplyTo: Option[ActorRef]    = None
  var lastDeleteSnapshotsReplyTo: Option[ActorRef] = None

  override def receiveRecover: Receive = {
    case event: Event =>
      log.debug("Replaying event: event=[{}]", event)
      recoveryTracking = recoveryTracking.withReplayedEvent(event)
    case SnapshotOffer(metadata, snapshot: Snapshot) =>
      log.debug("Loaded snapshot: metadata=[{}], snapshot=[{}]", metadata, snapshot)
      recoveryTracking = recoveryTracking.withOfferedSnapshot(snapshot)
    case RecoveryCompleted =>
      log.debug("Recovery completed: lastSequenceNr=[{}], recoveryTracking=[{}]", lastSequenceNr, recoveryTracking)
  }

  override def receiveCommand: Receive = {
    case GetRecoveryTracking(replyTo) =>
      replyTo ! recoveryTracking
    case PersistEvent(replyTo) =>
      val event = Event(lastSequenceNr + 1)
      log.debug("Persisting event: {}", event)
      persist(event) { _ =>
        replyTo ! Ack(lastSequenceNr)
      }
    case PersistTaggedEvent(tag, replyTo) =>
      val event       = Event(lastSequenceNr + 1)
      val taggedEvent = Tagged(event, Set(tag))
      log.debug("Persisting tagged event: [{}]", taggedEvent)
      persist(taggedEvent) { _ =>
        replyTo ! Ack(lastSequenceNr)
      }
    case PersistEventsAtomically(numOfEvents, replyTo) =>
      assert(numOfEvents >= 1)
      val events = for (i <- 1 to numOfEvents) yield {
        Event(lastSequenceNr + i)
      }
      log.debug("Persisting events atomically: {}", events)
      persistAll(events) { _ => }
      defer(NotUsed) { _ =>
        replyTo ! Ack(lastSequenceNr)
      }
    case SaveSnapshot(replyTo) =>
      val snapshot = Snapshot(lastSequenceNr)
      log.debug("Persisting snapshot: [{}]", snapshot)
      lastSaveSnapshotReplyTo = Some(replyTo)
      saveSnapshot(snapshot)
    case SaveSnapshotSuccess(metadata) =>
      log.debug("SaveSnapshotSuccess: metadata=[{}]", metadata)
      lastSaveSnapshotReplyTo.foreach(_.tell(Ack(metadata.sequenceNr), self))
      lastSaveSnapshotReplyTo = None
    case SaveSnapshotFailure(metadata, cause) =>
      log.info("SaveSnapshotFailure: metadata=[{}], cause=[{}]", metadata, cause)
      lastSaveSnapshotReplyTo = None
    case DeleteEventsTo(toSequenceNr: Long, replyTo) =>
      log.debug("Deleting events up to sequenceNr [{}]", toSequenceNr)
      lastDeleteEventsReplyTo = Some(replyTo)
      deleteMessages(toSequenceNr)
    case DeleteMessagesSuccess(toSequenceNr) =>
      log.debug("DeleteMessagesSuccess: toSequenceNr=[{}]", toSequenceNr)
      lastDeleteEventsReplyTo.foreach(_.tell(Ack(toSequenceNr), self))
      lastDeleteEventsReplyTo = None
    case DeleteMessagesFailure(cause, toSequenceNr) =>
      log.info("DeleteMessagesFailure: cause=[{}], toSequenceNr=[{}]", cause, toSequenceNr)
      lastDeleteEventsReplyTo = None
    case DeleteSnapshotsTo(toSequenceNr, replyTo) =>
      log.debug("Deleting snapshots up to sequenceNr [{}]", toSequenceNr)
      lastDeleteSnapshotsReplyTo = Some(replyTo)
      deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = toSequenceNr))
    case DeleteSnapshotsSuccess(criteria) =>
      log.debug("DeleteSnapshotsSuccess: criteria=[{}]", criteria)
      lastDeleteSnapshotsReplyTo.foreach(_.tell(Ack(criteria.maxSequenceNr), self))
      lastDeleteSnapshotsReplyTo = None
    case DeleteSnapshotsFailure(criteria, cause) =>
      log.info("DeleteSnapshotsFailure: criteria=[{}], cause=[{}]", criteria, cause)
      lastDeleteSnapshotsReplyTo = None
  }
}
