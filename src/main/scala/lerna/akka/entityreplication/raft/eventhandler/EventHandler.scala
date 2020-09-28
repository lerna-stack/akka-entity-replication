package lerna.akka.entityreplication.raft.eventhandler

import java.util.UUID

import akka.stream.scaladsl.Flow
import akka.{ Done, NotUsed }

import scala.concurrent.Future

trait EventHandler {
  def fetchOffsetUuid(): Future[Option[UUID]]
  def handleFlow(): Flow[EventEnvelope, Done, NotUsed]
}

object EventHandler {
  private[eventhandler] def tag: String = "raft-committed"
}

final case class EventEnvelope(offsetUuid: UUID, event: Any)
