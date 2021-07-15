package lerna.akka.entityreplication

import akka.actor.{ ActorRef, ExtendedActorSystem, Props }
import akka.event.Logging
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.persistence.query.scaladsl.CurrentEventsByTagQuery
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn

object RaftEventSourcedSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      lerna.akka.entityreplication.raft.multi-raft-roles = ["member-1", "member-2", "member-3"]
      lerna.akka.entityreplication.recovery-entity-timeout = 1s

      inmemory-journal {
        event-adapters {
          dummy-event-adapter = "lerna.akka.entityreplication.RaftEventSourcedSpec$DummyEventAdapter"
        }
        event-adapter-bindings {
          "lerna.akka.entityreplication.RaftEventSourcedSpec$DummyReplicationActor$DomainEvent" = dummy-event-adapter
        }
       }
      """))
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["member-1"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["member-2"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["member-3"]
  """))
}

class RaftEventSourcedSpecMultiJvmNode1 extends RaftEventSourcedSpec
class RaftEventSourcedSpecMultiJvmNode2 extends RaftEventSourcedSpec
class RaftEventSourcedSpecMultiJvmNode3 extends RaftEventSourcedSpec

object RaftEventSourcedSpec {

  object DummyReplicationActor {

    def props(): Props = Props(new DummyReplicationActor())

    sealed trait Command extends STMultiNodeSerializable {
      def id: String
    }

    final case class Increment(id: String, requestId: Int, amount: Int) extends Command
    final case class GetState(id: String)                               extends Command

    val extractEntityId: ReplicationRegion.ExtractEntityId = {
      case c: Command => (c.id, c)
    }

    val extractShardId: ReplicationRegion.ExtractShardId = {
      case c: Command => (Math.abs(c.id.hashCode) % 256).toString
    }

    sealed trait DomainEvent
    final case class Incremented(amount: Int, requestId: Int) extends DomainEvent with STMultiNodeSerializable

    final case class State(count: Int, knownRequestId: Set[Int]) extends STMultiNodeSerializable {
      def increment(amount: Int, requestId: Int): State = {
        assert(unknownRequestId(requestId))
        copy(
          count = count + amount,
          knownRequestId = knownRequestId + requestId,
        )
      }

      def unknownRequestId(requestId: Int): Boolean =
        !knownRequestId.contains(requestId)
    }
  }

  import DummyReplicationActor._

  @nowarn("msg=Use typed.ReplicatedEntityBehavior instead")
  class DummyReplicationActor extends ReplicationActor[State] {

    private[this] var state: State = State(count = 0, knownRequestId = Set.empty)

    override def currentState: State = state

    override def receiveReplica: Receive = {
      case SnapshotOffer(snapshot: State) => state = snapshot
      case event: Incremented             => updateState(event)
    }

    override def receiveCommand: Receive = {
      case increment: Increment =>
        if (state.unknownRequestId(increment.requestId)) {
          replicate(Incremented(increment.amount, increment.requestId)) { event =>
            updateState(event)
            sender() ! state
          }
        } else {
          ensureConsistency {
            // have already processed the request, so it must not issue an event
            sender() ! state
          }
        }
      case _: GetState =>
        ensureConsistency {
          sender() ! state
        }
    }

    private[this] def updateState(event: Incremented): Unit = {
      state = state.increment(event.amount, event.requestId)
      println(s"state updated: $state")
    }
  }

  object DummyEventAdapter {
    val transactionTag = "dummy-transaction"
  }

  class DummyEventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {
    import DummyEventAdapter._

    private[this] val log = Logging(system, getClass)

    override def manifest(event: Any): String = ""

    override def toJournal(event: Any): Any = {
      event match {
        case event: DummyReplicationActor.DomainEvent =>
          Tagged(event, Set(transactionTag))
        case event =>
          log.warning("unexpected event: {}", event)
          event
      }
    }
  }
}

@nowarn("msg=method start in class ClusterReplication is deprecated")
class RaftEventSourcedSpec extends MultiNodeSpec(RaftEventSourcedSpecConfig) with STMultiNodeSpec {

  import RaftEventSourcedSpec._
  import RaftEventSourcedSpecConfig._

  override def initialParticipants: Int = roles.size

  private[this] val typeName = "test"

  private[this] var clusterReplication: ActorRef = null

  "EventSourced" should {

    "wait for all nodes to join the cluster" in {
      joinCluster(node1, node2, node3)
    }

    "produce committed events for reading by PersistenceQuery" in {
      runOn(node1, node2, node3) {
        clusterReplication = planAutoKill {
          ClusterReplication(system).start(
            typeName,
            entityProps = DummyReplicationActor.props(),
            settings = ClusterReplicationSettings(system),
            DummyReplicationActor.extractEntityId,
            DummyReplicationActor.extractShardId,
          )
        }
      }
      val entityId   = generateUniqueEntityId()
      val requestId1 = generateUniqueRequestId()
      val requestId2 = generateUniqueRequestId()
      runOn(node2) {
        awaitAssert {
          clusterReplication ! DummyReplicationActor.Increment(entityId, requestId1, amount = 3)
          expectMsgType[DummyReplicationActor.State].knownRequestId should contain(requestId1)
        }
        awaitAssert {
          clusterReplication ! DummyReplicationActor.Increment(entityId, requestId2, amount = 10)
          expectMsgType[DummyReplicationActor.State].knownRequestId should contain(requestId2)
        }

        val readJournal =
          PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](
            readJournalPluginId = "lerna.akka.entityreplication.util.persistence.query.proxy",
          )

        val source = readJournal
          .currentEventsByTag(DummyEventAdapter.transactionTag, Offset.noOffset)

        awaitAssert {
          import DummyReplicationActor.Incremented
          val result = source.runFold(Seq.empty[EventEnvelope])(_ :+ _).await.collect {
            case EventEnvelope(_, _, _, event: Incremented) => event
          }

          result should contain theSameElementsInOrderAs Seq(
            Incremented(amount = 3, requestId1),
            Incremented(amount = 10, requestId2),
          )
        }
      }
    }
  }

  private[this] val uniqueIdCounter = new AtomicInteger(0)

  private[this] def generateUniqueRequestId(): Int = uniqueIdCounter.incrementAndGet()

  private[this] def generateUniqueEntityId(): String = uniqueIdCounter.incrementAndGet().toString
}
