package lerna.akka.entityreplication

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{ ActorRef, ActorSystem, OneForOneStrategy, Props }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.ReplicationActorSpec.{ config, ExampleReplicationActor }
import lerna.akka.entityreplication.model.EntityInstanceId
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, Term }
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftProtocol }
import java.util.concurrent.atomic.AtomicInteger

object ReplicationActorSpec {

  private val config = ConfigFactory
    .parseString("""
      |lerna.akka.entityreplication.recovery-entity-timeout = 2s // < test timeout 3s
      |""".stripMargin)
    .withFallback(ConfigFactory.load())

  object ExampleReplicationActor {
    def props: Props = Props(new ExampleReplicationActor)

    sealed trait Command
    case class Count() extends Command
    case class Break() extends Command

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
      case Break() =>
        throw new RuntimeException("bang!")
    }

    override def currentState: Int = count

    def updateState(event: Event): Unit =
      event match {
        case Counted() => count += 1
      }
  }
}

class ReplicationActorSpec extends TestKit(ActorSystem("ReplicationActorSpec", config)) with ActorSpec {
  import RaftProtocol._
  import ExampleReplicationActor._

  "ReplicationActor" should {

    def createReplicationActor(parent: TestProbe): ActorRef =
      planAutoKill {
        val replicationActor = parent.childActorOf(
          ExampleReplicationActor.props,
          OneForOneStrategy() {
            case _: RuntimeException => Restart
          },
        )
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

    "replace instanceId when it restarted" in {
      val raftActorProbe   = TestProbe()
      val replicationActor = createReplicationActor(raftActorProbe)

      replicationActor ! Count()
      val r1 = raftActorProbe.expectMsgType[Replicate]
      r1.replyTo ! createReplicationSucceeded(Counted(), r1.instanceId)
      replicationActor ! Break() // ReplicationActor will restart
      // recovery start
      raftActorProbe.expectMsgType[RequestRecovery]
      replicationActor ! RecoveryState(Seq(), None)
      // recovery complete
      replicationActor ! Count()
      val r2 = raftActorProbe.expectMsgType[Replicate]
      r2.instanceId should not be (r1.instanceId)
    }

    "ignore ReplicationSucceeded which has old instanceId" in {
      val raftActorProbe   = TestProbe()
      val replicationActor = createReplicationActor(raftActorProbe)

      replicationActor ! Count()
      val r = raftActorProbe.expectMsgType[Replicate]

      val oldEntityInstanceId = r.instanceId.map(id => EntityInstanceId(id.underlying - 1))
      r.replyTo ! createReplicationSucceeded(Counted(), oldEntityInstanceId) // this response will be ignored

      replicationActor ! Count()
      raftActorProbe.expectNoMessage() // the command is stashed
      r.replyTo ! createReplicationSucceeded(Counted(), r.instanceId)
      raftActorProbe.expectMsgType[Replicate]
    }

    "reboot and request Recovery again after RecoveryTimeout" in {
      val testProbe      = TestProbe()
      val raftActorProbe = TestProbe()
      planAutoKill {
        raftActorProbe.childActorOf(
          ExampleReplicationActor.props,
          OneForOneStrategy() {
            case runtimeException: RuntimeException =>
              testProbe.ref ! runtimeException
              Restart
          },
        )
      }
      // Do not send RecoveryState to replicationActor
      raftActorProbe.expectMsgType[RequestRecovery]
      testProbe.expectMsgType[EntityRecoveryTimeoutException]
      raftActorProbe.expectMsgType[RequestRecovery]
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
