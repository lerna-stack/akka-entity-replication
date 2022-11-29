package lerna.akka.entityreplication.typed

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.receptionist.{ Receptionist, ServiceKey }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object ReplicatedEntitySnapshotMultiNodeSpecConfig extends MultiNodeConfig {
  val controller: RoleName = role("controller")
  val node1: RoleName      = role("node1")
  val node2: RoleName      = role("node2")
  val node3: RoleName      = role("node3")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-2"),
    node3 -> MemberIndex("member-3"),
  )

  testTransport(true)

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString("""
      // triggers compaction each event replications
      lerna.akka.entityreplication.raft.compaction.log-size-threshold = 3
      lerna.akka.entityreplication.raft.compaction.preserve-log-size = 2
      lerna.akka.entityreplication.raft.compaction.log-size-check-interval = 0.1s
      """))
      .withValue(
        "lerna.akka.entityreplication.raft.multi-raft-roles",
        ConfigValueFactory.fromIterable(memberIndexes.values.map(_.role).toSet.asJava),
      )
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node1).role}"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2).role}"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node3).role}"]
  """))
}

class ReplicatedEntitySnapshotMultiNodeSpecMultiJvmController extends ReplicatedEntitySnapshotMultiNodeSpec
class ReplicatedEntitySnapshotMultiNodeSpecMultiJvmNode1      extends ReplicatedEntitySnapshotMultiNodeSpec
class ReplicatedEntitySnapshotMultiNodeSpecMultiJvmNode2      extends ReplicatedEntitySnapshotMultiNodeSpec
class ReplicatedEntitySnapshotMultiNodeSpecMultiJvmNode3      extends ReplicatedEntitySnapshotMultiNodeSpec

class ReplicatedEntitySnapshotMultiNodeSpec
    extends MultiNodeSpec(ReplicatedEntitySnapshotMultiNodeSpecConfig)
    with STMultiNodeSpec {

  import ReplicatedEntitySnapshotMultiNodeSpec._
  import ReplicatedEntitySnapshotMultiNodeSpecConfig._

  private[this] val typedSystem = system.toTyped

  private[this] val clusterReplication = ClusterReplication(typedSystem)

  private[this] val actorTestKit = ActorTestKit(typedSystem)

  "ReplicatedEntity" should {

    "wait for all nodes to join the cluster" in {
      joinCluster(controller, node1, node2, node3)
    }

    "synchronize all member state even if the Follower could not receive logs by compaction in a Leader" in {

      val replyTo = actorTestKit.createTestProbe[DummyEntity.State]()

      var entityRef: ReplicatedEntityRef[DummyEntity.Command] = null

      runOn(node1, node2, node3) {
        clusterReplication.init(DummyEntity())
        entityRef = clusterReplication.entityRefFor(DummyEntity.typeKey, createSeqEntityId())
      }

      val transactionSeqGenerator = new AtomicInteger(1)

      val transactionSeq1 = transactionSeqGenerator.incrementAndGet()
      runOn(node1) {
        awaitAssert {
          // update state
          entityRef ! DummyEntity.Increment(transactionSeq1, amount = 1, replyTo.ref)
          replyTo.receiveMessage(500.millis).count should be(1)
        }
      }
      enterBarrier("sent a command")

      var actorRef: ActorRef[DummyEntity.Command] = null
      runOn(node1, node2, node3) {
        awaitAssert {
          actorRef = findEntityActorRef()
          actorRef ! DummyEntity.GetState(replyTo.ref)
          replyTo.receiveMessage(500.millis).count should be(1)
        }
      }
      enterBarrier("all entities applied an event")

      isolate(node3, excludes = Set(controller))

      (1 to 4).foreach { _ =>
        val transactionSeq = transactionSeqGenerator.incrementAndGet()
        runOn(node1) {
          // update state
          var latestCount = 1
          awaitAssert {
            entityRef ! DummyEntity.Increment(transactionSeq, amount = 1, replyTo.ref)
            val reply = replyTo.receiveMessage(500.millis)
            reply.count should be > latestCount
            latestCount = reply.count
          }
        }
      }
      enterBarrier("sent additional commands")

      runOn(node1, node2) {
        // entity which has not been isolated applied all events
        awaitAssert {
          actorRef ! DummyEntity.GetState(replyTo.ref)
          replyTo.receiveMessage(500.millis).count should be(5)
        }
      }
      runOn(node3) {
        // entity which has been isolated did not apply any events
        awaitAssert {
          actorRef ! DummyEntity.GetState(replyTo.ref)
          replyTo.receiveMessage(500.millis).count should be(1)
        }
      }
      enterBarrier("entity which has not been isolated applied all events")

      releaseIsolation(node3)

      val transactionSeq2 = transactionSeqGenerator.incrementAndGet()
      runOn(node1, node2, node3) {
        awaitAssert {
          // attempt to create entities on all nodes
          entityRef ! DummyEntity.Increment(transactionSeq2, amount = 0, replyTo.ref)
          replyTo.receiveMessage(500.millis)
          // All ReplicationActor states will eventually be the same as any other after the isolation is resolved
          actorRef = findEntityActorRef()
          actorRef ! DummyEntity.GetState(replyTo.ref)
          replyTo.receiveMessage(500.millis).count should be(5)
        }
      }
    }
  }

  private[this] val idGenerator                 = new AtomicInteger(0)
  private[this] def createSeqEntityId(): String = s"replication-${idGenerator.incrementAndGet()}"

  private[this] def findEntityActorRef(): ActorRef[DummyEntity.Command] = {
    val subscriber = actorTestKit.createTestProbe[Receptionist.Listing]()
    typedSystem.receptionist ! Receptionist.Find(DummyEntity.serviceKey, subscriber.ref)
    subscriber.receiveMessage().serviceInstances(DummyEntity.serviceKey).head
  }
}

object ReplicatedEntitySnapshotMultiNodeSpec {

  object DummyEntity {
    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("Dummy")
    val serviceKey: ServiceKey[Command]           = ServiceKey("DummyService")

    sealed trait Command                                                                   extends STMultiNodeSerializable
    final case class Increment(transactionSeq: Int, amount: Int, replyTo: ActorRef[State]) extends Command
    final case class GetState(replyTo: ActorRef[State])                                    extends Command

    sealed trait Event                                             extends STMultiNodeSerializable
    final case class Incremented(transactionSeq: Int, amount: Int) extends Event

    final case class State(latestTransactionSeq: Int, count: Int) extends STMultiNodeSerializable {

      def onMessage(message: Command): Effect[Event, State] =
        message match {
          case Increment(transactionSeq, amount, replyTo) =>
            if (transactionSeq > latestTransactionSeq) {
              Effect
                .replicate(Incremented(transactionSeq, amount))
                .thenReply(replyTo)(identity)
            } else {
              Effect.none
                .thenReply(replyTo)(identity)
            }
          case GetState(replyTo) =>
            replyTo ! this // unsafe! This is implemented for showing this entity status directly
            // If we use Effect.reply, we are going to receive only the Leader state (for ensuring consistency)
            Effect.noReply
        }

      def applyEvent(event: Event): State =
        event match {
          case Incremented(transactionSeq, amount) => copy(transactionSeq, count = count + amount)
        }
    }

    def apply(): ReplicatedEntity[Command, ReplicationEnvelope[Command]] =
      ReplicatedEntity(typeKey)(entityContext =>
        Behaviors.setup { context =>
          context.system.receptionist ! Receptionist.Register(serviceKey, context.self)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(latestTransactionSeq = 0, count = 0),
            commandHandler = _ onMessage _,
            eventHandler = _ applyEvent _,
          )
        },
      )
  }
}
