package lerna.akka.entityreplication.util.persistence.query.proxy.scaladsl

import akka.NotUsed
import akka.actor.{ ActorNotFound, ActorRef, ActorSystem, Address, ExtendedActorSystem, RootActorPath }
import akka.event.Logging
import akka.pattern.ask
import akka.persistence.query.scaladsl._
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.config.Config
import lerna.akka.entityreplication.util.persistence.query.proxy.ReadJournalPluginProxyActor

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object ReadJournalPluginProxy {

  val identifier: String = "lerna.akka.entityreplication.util.persistence.query.proxy"

  def setTargetLocation(system: ActorSystem, address: Address): Unit = {
    PersistenceQuery(system)
      .readJournalFor[ReadJournalPluginProxy](identifier).setTargetLocation(address)
  }
}

class ReadJournalPluginProxy(config: Config)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery {

  import system.dispatcher

  private[this] val log = Logging(system, this.getClass)

  private[this] val actorName = "ReadJournalProxy"

  private[this] implicit val timeout: Timeout = Timeout(10.seconds)

  private[this] val localTargetProxy = system.actorOf(ReadJournalPluginProxyActor.props(config), actorName)

  private[this] val targetProxySelection = new AtomicReference[Option[ActorRef]](None)

  def setTargetLocation(address: Address): Unit = {
    val selection = system.actorSelection(RootActorPath(address) / "user" / actorName)
    selection.resolveOne().onComplete {
      case Success(ref) =>
        log.info("Found target read-journal at [{}]", address)
        targetProxySelection.set(Option(ref))
      case Failure(_: ActorNotFound) =>
        log.error("Target read-journal not found at [{}]", address)
      case Failure(exception) =>
        log.error(exception, "Failed to initialize")
    }
  }

  private[this] def targetProxy: ActorRef =
    targetProxySelection.get().getOrElse {
      log.warning("Target read-journal not initialized. Use `ReadJournalPluginProxy.setTargetLocation`")
      localTargetProxy
    }

  override def currentPersistenceIds(): Source[String, NotUsed] =
    Source
      .future(
        (targetProxy ? ReadJournalPluginProxyActor.CurrentPersistentIds())
          .mapTo[ReadJournalPluginProxyActor.CurrentPersistentIdsResponse],
      )
      .flatMapConcat(_.ref)

  override def persistenceIds(): Source[String, NotUsed] =
    Source
      .future(
        (targetProxy ? ReadJournalPluginProxyActor.PersistentIds())
          .mapTo[ReadJournalPluginProxyActor.PersistentIdsResponse],
      )
      .flatMapConcat(_.ref)

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
  ): Source[EventEnvelope, NotUsed] =
    Source
      .future(
        (targetProxy ? ReadJournalPluginProxyActor
          .CurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr))
          .mapTo[ReadJournalPluginProxyActor.CurrentEventsByPersistenceIdResponse],
      )
      .flatMapConcat(_.ref)

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
  ): Source[EventEnvelope, NotUsed] =
    Source
      .future(
        (targetProxy ? ReadJournalPluginProxyActor.EventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr))
          .mapTo[ReadJournalPluginProxyActor.EventsByPersistenceIdResponse],
      )
      .flatMapConcat(_.ref)

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    Source
      .future(
        (targetProxy ? ReadJournalPluginProxyActor.CurrentEventsByTag(tag, offset))
          .mapTo[ReadJournalPluginProxyActor.CurrentEventsByTagResponse],
      )
      .flatMapConcat(_.ref)

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    Source
      .future(
        (targetProxy ? ReadJournalPluginProxyActor.EventsByTag(tag, offset))
          .mapTo[ReadJournalPluginProxyActor.EventsByTagResponse],
      )
      .flatMapConcat(_.ref)
}
