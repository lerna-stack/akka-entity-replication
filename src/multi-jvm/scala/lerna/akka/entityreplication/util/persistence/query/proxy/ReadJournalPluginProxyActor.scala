package lerna.akka.entityreplication.util.persistence.query.proxy

import akka.actor.{ Actor, ActorLogging, Props }
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentEventsByTagQuery,
  CurrentPersistenceIdsQuery,
  EventsByPersistenceIdQuery,
  EventsByTagQuery,
  PersistenceIdsQuery,
  ReadJournal,
}
import akka.stream.SourceRef
import akka.stream.scaladsl.StreamRefs
import com.typesafe.config.Config

object ReadJournalPluginProxyActor {

  def props(readJournalPluginProxyConfig: Config): Props =
    Props(new ReadJournalPluginProxyActor(readJournalPluginProxyConfig))

  sealed trait Command

  final case class CurrentPersistentIds() extends Command
  final case class PersistentIds()        extends Command
  final case class CurrentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long)
      extends Command
  final case class EventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long)
      extends Command
  final case class CurrentEventsByTag(tag: String, offset: Offset) extends Command
  final case class EventsByTag(tag: String, offset: Offset)        extends Command

  sealed trait Response
  final case class CurrentPersistentIdsResponse(ref: SourceRef[String])                extends Response
  final case class PersistentIdsResponse(ref: SourceRef[String])                       extends Response
  final case class CurrentEventsByPersistenceIdResponse(ref: SourceRef[EventEnvelope]) extends Response
  final case class EventsByPersistenceIdResponse(ref: SourceRef[EventEnvelope])        extends Response
  final case class CurrentEventsByTagResponse(ref: SourceRef[EventEnvelope])           extends Response
  final case class EventsByTagResponse(ref: SourceRef[EventEnvelope])                  extends Response
}

class ReadJournalPluginProxyActor(readJournalPluginProxyConfig: Config) extends Actor with ActorLogging {
  import ReadJournalPluginProxyActor._

  private[this] type TargetReadJournal = ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery

  private[this] val targetReadJournalPluginId = readJournalPluginProxyConfig.getString("target-read-journal-plugin")

  private[this] val readJournal =
    PersistenceQuery(context.system).readJournalFor[TargetReadJournal](targetReadJournalPluginId)

  import context.system

  override def receive: Receive = {
    case CurrentPersistentIds() =>
      sender() ! CurrentPersistentIdsResponse(
        ref = readJournal.currentPersistenceIds().runWith(StreamRefs.sourceRef()),
      )
    case PersistentIds() =>
      sender() ! PersistentIdsResponse(
        ref = readJournal.persistenceIds().runWith(StreamRefs.sourceRef()),
      )
    case CurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr) =>
      sender() ! CurrentEventsByPersistenceIdResponse(
        ref = readJournal
          .currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).runWith(StreamRefs.sourceRef()),
      )
    case EventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr) =>
      sender() ! EventsByPersistenceIdResponse(
        ref =
          readJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).runWith(StreamRefs.sourceRef()),
      )
    case CurrentEventsByTag(tag, offset) =>
      sender() ! CurrentEventsByTagResponse(
        ref = readJournal.currentEventsByTag(tag, offset).runWith(StreamRefs.sourceRef()),
      )
    case EventsByTag(tag, offset) =>
      sender() ! EventsByTagResponse(
        ref = readJournal.eventsByTag(tag, offset).runWith(StreamRefs.sourceRef()),
      )
  }
}
