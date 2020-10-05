package lerna.akka.entityreplication.raft.eventhandler

import akka.actor.typed.ActorSystem
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.SourceProvider
import com.typesafe.config.Config

object EntityReplicationEventSource {
  private[eventhandler] def tag: String = "raft-committed"

  // TODO: 複数 Raft(typeName) に対応するために typeName ごとに cassandra-query-journal を分ける
  private def readJournalPluginId(config: Config) =
    config.getString("lerna.akka.entityreplication.raft.eventhandler.persistence.query.plugin")

  def sourceProvider[E](implicit system: ActorSystem[_]): SourceProvider[Offset, EventEnvelope[E]] =
    EventSourcedProvider
      .eventsByTag[E](system = system, readJournalPluginId = readJournalPluginId(system.settings.config), tag = tag)
}
