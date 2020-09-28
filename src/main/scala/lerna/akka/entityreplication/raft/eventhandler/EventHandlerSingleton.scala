package lerna.akka.entityreplication.raft.eventhandler

import akka.Done
import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  ActorSystem,
  NoSerializationVerificationNeeded,
  PoisonPill,
  Props,
  Status,
}
import akka.cluster.singleton.{ ClusterSingletonManager, ClusterSingletonManagerSettings }
import akka.pattern.pipe
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{ Offset, PersistenceQuery, TimeBasedUUID }
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink }
import akka.stream.{ KillSwitch, KillSwitches, Materializer }
import lerna.akka.entityreplication.util.JavaDurationConverters._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

object EventHandlerSingleton {
  def props(typeName: String, eventHandler: EventHandler): Props = {
    Props(new EventHandlerSingleton(typeName, eventHandler))
  }

  final case class EventHandlerStreamStarted(killSwitch: KillSwitch) extends NoSerializationVerificationNeeded

  final case object End extends NoSerializationVerificationNeeded

  def startAsSingleton(typeName: String, system: ActorSystem, eventHandler: EventHandler): ActorRef = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(typeName, eventHandler),
        terminationMessage = End,
        settings = ClusterSingletonManagerSettings(system),
      ),
      name = s"raft-committed-event-handler-$typeName",
    )
  }
}
class EventHandlerSingleton(typeName: String, eventHandler: EventHandler) extends Actor with ActorLogging {
  import EventHandlerSingleton._

  private implicit val ec: ExecutionContext = context.dispatcher

  log.info("EventHandlerSingleton start: {}", self.path)

  var maybeKillSwitch: Option[KillSwitch] = None

  override def receive: Receive = {
    case Done =>
      log.info("complete eventHandlerStream Success")
    case Status.Failure(exception) =>
      log.error(exception, "complete eventHandlerStream Failure")
      throw exception
    case EventHandlerStreamStarted(killSwitch) =>
      maybeKillSwitch = Option(killSwitch)
    case End =>
      maybeKillSwitch.foreach(_.shutdown())

      // context stop self すると stream 終了時の future pipeTo self が deadLetterになってしまうため遅らせる
      val stopSelfDelay = context.system.settings.config
        .getDuration("lerna.akka.entityreplication.raft.eventhandler.event-handler-singleton.stop-self-delay").asScala
      context.system.scheduler.scheduleOnce(stopSelfDelay, self, PoisonPill)
  }

  implicit val materializer: Materializer = Materializer(context)
  generateEventHandlerStream().onComplete {
    case Success(runnableGraph) =>
      val (killSwitch, streamFinishedSignal) = runnableGraph.run()
      self ! EventHandlerStreamStarted(killSwitch)
      streamFinishedSignal pipeTo self
    case Failure(exception) =>
      self ! Status.Failure(new RuntimeException("EventHandlerStreamの作成に失敗しました", exception))
  }

  private def generateEventHandlerStream(): Future[RunnableGraph[(KillSwitch, Future[Done])]] = {
    eventHandler.fetchOffsetUuid().map { maybeUuid =>
      val offset = maybeUuid.fold(Offset.noOffset)(Offset.timeBasedUUID)
      // TODO: 複数 Raft(typeName) に対応するために typeName ごとに cassandra-query-journal を分ける
      val readJournalPluginId           = "lerna.akka.entityreplication.raft.eventhandler.cassandra-plugin.query"
      val readJournal: EventsByTagQuery = PersistenceQuery(context.system).readJournalFor(readJournalPluginId)

      readJournal
        .eventsByTag(EventHandler.tag, offset)
        .viaMat(KillSwitches.single)(Keep.right)
        .map(eventEnvelope => {
          val maybeCommittedEvent = eventEnvelope.event match {
            case committedEvent: CommittedEvent => Option(committedEvent)
            case event =>
              log.warning(s"eventが CommittedEvent ではないため処理をスキップします。フレームワークチームに問い合わせてください。event: ${event}")
              None
          }
          (eventEnvelope.offset, maybeCommittedEvent)
        })
        .collect {
          // CommittedEvent 以外(None) と InternalEvent を処理しない
          case (offset, Some(domainEvent: DomainEvent)) => (offset, domainEvent)
        }
        .map {
          case (offset, domainEvent) =>
            offset match {
              case TimeBasedUUID(uuid) => EventEnvelope(uuid, domainEvent.event)
              case offset =>
                throw new IllegalStateException(
                  s"offsetが TimeBasedUUID ではないため処理を継続できません。フレームワークチームに問い合わせてください。offset: ${offset}",
                )
            }
        }
        .via(eventHandler.handleFlow())
        .toMat(Sink.ignore)(Keep.both)
    }
  }
}
