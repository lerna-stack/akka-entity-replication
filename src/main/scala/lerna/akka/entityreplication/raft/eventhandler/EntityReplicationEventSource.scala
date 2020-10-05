package lerna.akka.entityreplication.raft.eventhandler

import akka.actor.typed.ActorSystem
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.SourceProvider

object EntityReplicationEventSource {
  private[eventhandler] def tag: String = "raft-committed"

  // TODO: 複数 Raft(typeName) に対応するために typeName ごとに cassandra-query-journal を分ける
  private val readJournalPluginId = "lerna.akka.entityreplication.raft.eventhandler.cassandra-plugin.query"

  def sourceProvider[E](implicit system: ActorSystem[_]): SourceProvider[Offset, EventEnvelope[E]] =
    EventSourcedProvider
      .eventsByTag[E](system = system, readJournalPluginId = readJournalPluginId, tag = tag)
}
