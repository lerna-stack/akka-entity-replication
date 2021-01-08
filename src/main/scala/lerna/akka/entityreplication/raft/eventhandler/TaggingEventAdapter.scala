package lerna.akka.entityreplication.raft.eventhandler

import akka.actor.ExtendedActorSystem
import akka.persistence.journal.{ Tagged, WriteEventAdapter }

class TaggingEventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {
  override def manifest(event: Any): String = "" // when no manifest needed, return ""

  override def toJournal(event: Any): Any = {
    event match {
      case InternalEvent => event
      case _ =>
        val tags: Set[String] = Set(EntityReplicationEventSource.tag)
        Tagged(event, tags)
    }
  }
}
