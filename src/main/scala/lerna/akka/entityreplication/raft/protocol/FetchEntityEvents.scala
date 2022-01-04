package lerna.akka.entityreplication.raft.protocol

import akka.actor.typed.ActorRef
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex }
import lerna.akka.entityreplication.typed.ClusterReplication.ShardCommand

private[entityreplication] final case class FetchEntityEvents(
    entityId: NormalizedEntityId,
    from: LogEntryIndex,
    to: LogEntryIndex,
    replyTo: ActorRef[FetchEntityEventsResponse],
) extends ShardCommand

private[entityreplication] final case class FetchEntityEventsResponse(events: Seq[LogEntry])
