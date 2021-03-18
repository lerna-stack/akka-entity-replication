package lerna.akka.entityreplication.raft.persistence

import akka.persistence.journal.{ EventAdapter, EventSeq, Tagged }
import lerna.akka.entityreplication.raft.RaftActor.CompactionCompleted

class RaftEventAdapter extends EventAdapter {

  override def manifest(event: Any): String = "" // No need

  override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single(event)

  override def toJournal(event: Any): Any = {
    event match {
      case event: CompactionCompleted =>
        Tagged(event, Set(CompactionCompletedTag(event.memberIndex, event.shardId).toString))
      case event => event
    }
  }
}
