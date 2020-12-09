package lerna.akka.entityreplication.raft
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.ReplicationActor
import lerna.akka.entityreplication.model.EntityInstanceId
import lerna.akka.entityreplication.raft.ReplicationActorSpec.ExampleReplicationActor
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, Term }

object ReplicationActorSpec {

  object ExampleReplicationActor {
    def props: Props = Props(new ExampleReplicationActor)

    sealed trait Command
    case class Count() extends Command

    sealed trait Event
    case class Counted() extends Event
  }

  class ExampleReplicationActor extends ReplicationActor[Int] {
    import ExampleReplicationActor._

    private[this] var count: Int = 0

    override def receiveReplica: Receive = {
      case event: Event => updateState(event)
    }

    override def receiveCommand: Receive = {
      case Count() =>
        replicate(Counted()) { e =>
          updateState(e)
          sender() ! e
        }
    }

    override def currentState: Int = count

    def updateState(event: Event): Unit =
      event match {
        case Counted() => count += 1
      }
  }
}

class ReplicationActorSpec extends TestKit(ActorSystem("ReplicationActorSpec")) with ActorSpec {
  import RaftProtocol._
  import ExampleReplicationActor._

  "ReplicationActor" should {

    def createReplicationActor(parent: TestProbe): ActorRef =
      planAutoKill {
        val replicationActor = parent.childActorOf(ExampleReplicationActor.props)
        parent.expectMsgType[RequestRecovery]
        replicationActor ! RecoveryState(Seq(), None)
        replicationActor
      }

    "withhold to process commands while replicating (while to receive ReplicationSucceeded)" in {
      val raftActorProbe   = TestProbe()
      val replicationActor = createReplicationActor(raftActorProbe)

      replicationActor ! Count()
      val r = raftActorProbe.expectMsgType[Replicate]
      replicationActor ! Count()
      raftActorProbe.expectNoMessage() // the command is stashed
      r.replyTo ! createReplicationSucceeded(Counted(), r.instanceId)
      raftActorProbe.expectMsgType[Replicate]
    }

    "withhold to process commands while replicating (while to receive Replica)" in {
      val raftActorProbe   = TestProbe()
      val replicationActor = createReplicationActor(raftActorProbe)

      replicationActor ! Count()
      raftActorProbe.expectMsgType[Replicate]
      replicationActor ! Count()
      raftActorProbe.expectNoMessage() // the command is stashed
      replicationActor ! Replica(createLogEntry(Counted()))
      raftActorProbe.expectMsgType[Replicate]
    }
  }

  private[this] val logEntrySeq = new AtomicInteger(1)

  private[this] def createReplicationSucceeded(
      event: Any,
      instanceId: Option[EntityInstanceId],
  ): ReplicationSucceeded = {
    ReplicationSucceeded(event, LogEntryIndex(logEntrySeq.getAndIncrement()), instanceId)
  }

  private[this] def createLogEntry(event: Any): LogEntry = {
    LogEntry(LogEntryIndex(logEntrySeq.getAndIncrement()), EntityEvent(None, event), Term.initial())
  }
}
