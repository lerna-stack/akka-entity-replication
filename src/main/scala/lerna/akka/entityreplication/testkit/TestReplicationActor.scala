package lerna.akka.entityreplication.testkit

import akka.actor.{ Actor, Props }
import lerna.akka.entityreplication.ReplicationRegion.Passivate
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.testkit.TestReplicationActorProps._

protected[testkit] class TestReplicationActor(replicationActorProvider: ReplicationActorProvider) extends Actor {
  import lerna.akka.entityreplication.raft.RaftProtocol._

  private[this] val replicationActor = context.actorOf(Props(replicationActorProvider))

  override def receive: Receive = active(LogEntryIndex(1))

  def active(dummyLogEntryIndex: LogEntryIndex): Receive = {
    case _: RequestRecovery =>
      sender() ! RecoveryState(events = Seq(), snapshot = None)
    case replicate: Replicate =>
      sender() ! ReplicationSucceeded(replicate.event, dummyLogEntryIndex, replicate.instanceId)
      context.become(active(dummyLogEntryIndex.next()))
    case passivate: Passivate =>
      context.actorSelection(passivate.entityPath) ! passivate.stopMessage
    case message =>
      replicationActor forward Command(message)
  }
}
