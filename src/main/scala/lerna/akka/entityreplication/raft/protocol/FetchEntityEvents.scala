package lerna.akka.entityreplication.raft.protocol

import akka.actor.typed.ActorRef
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex }

private[entityreplication] final case class FetchEntityEvents(
    entityId: NormalizedEntityId,
    from: LogEntryIndex,
    to: LogEntryIndex,
    replyTo: ActorRef[FetchEntityEventsResponse],
)

private[entityreplication] final case class FetchEntityEventsResponse(events: Seq[LogEntry])
