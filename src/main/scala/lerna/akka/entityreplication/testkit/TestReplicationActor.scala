package lerna.akka.entityreplication.testkit

import akka.actor.{ Actor, Props, Terminated }
import lerna.akka.entityreplication.ReplicationRegion.Passivate
import lerna.akka.entityreplication.raft.model.LogEntryIndex

protected[testkit] class TestReplicationActor(replicationActorProps: Props) extends Actor {
  import lerna.akka.entityreplication.raft.RaftProtocol._

  private[this] val replicationActor = context.watch(context.actorOf(replicationActorProps))

  override def receive: Receive = active(LogEntryIndex(1))

  def active(dummyLogEntryIndex: LogEntryIndex): Receive = {
    case _: RequestRecovery =>
      sender() ! RecoveryState(events = Seq(), snapshot = None)
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
      replicationActor forward Command(message)
  }
}
