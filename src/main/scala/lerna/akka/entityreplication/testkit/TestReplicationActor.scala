package lerna.akka.entityreplication.testkit

import akka.actor.{ Actor, Props, Terminated }
import lerna.akka.entityreplication.ReplicationRegion.Passivate
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol

protected[testkit] class TestReplicationActor(replicationActorProps: Props) extends Actor {
  import lerna.akka.entityreplication.raft.RaftProtocol._

  private[this] val replicationActor = context.watch(context.actorOf(replicationActorProps))

  replicationActor ! Activate(self, recoveryIndex = LogEntryIndex.initial().next())

  override def receive: Receive = active(LogEntryIndex(1))

  def active(dummyLogEntryIndex: LogEntryIndex): Receive = {
    case fetchSnapshot: SnapshotProtocol.FetchSnapshot =>
      fetchSnapshot.replyTo ! SnapshotProtocol.SnapshotNotFound(fetchSnapshot.entityId)
    case fetchEvents: FetchEntityEvents =>
      fetchEvents.replyTo ! FetchEntityEventsResponse(Seq())
    case replicate: Replicate =>
      val sender = replicate.originSender.getOrElse(self)
      replicate.replyTo.tell(ReplicationSucceeded(replicate.event, dummyLogEntryIndex, replicate.instanceId), sender)
      context.become(active(dummyLogEntryIndex.next()))
    case passivate: Passivate =>
      if (passivate.entityPath == replicationActor.path) {
        replicationActor ! passivate.stopMessage
      } else {
        context.system.deadLetters ! passivate
      }
    case Terminated(`replicationActor`) =>
      context.stop(self)
    case message =>
      replicationActor forward ProcessCommand(message)
  }
}
