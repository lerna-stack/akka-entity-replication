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

object ReplicatedEntityMultiNodeSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-2"),
    node3 -> MemberIndex("member-3"),
  )

  commonConfig(
    debugConfig(false)
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

class ReplicatedEntityMultiNodeSpecMultiJvmNode1 extends ReplicatedEntityMultiNodeSpec
class ReplicatedEntityMultiNodeSpecMultiJvmNode2 extends ReplicatedEntityMultiNodeSpec
class ReplicatedEntityMultiNodeSpecMultiJvmNode3 extends ReplicatedEntityMultiNodeSpec

class ReplicatedEntityMultiNodeSpec extends MultiNodeSpec(ReplicatedEntityMultiNodeSpecConfig) with STMultiNodeSpec {

  import ReplicatedEntityMultiNodeSpec._
  import ReplicatedEntityMultiNodeSpecConfig._

  private[this] val typedSystem = system.toTyped

  private[this] val clusterReplication = ClusterReplication(typedSystem)

  private[this] val actorTestKit = ActorTestKit(typedSystem)

  "ReplicatedEntity" should {

    "wait for all nodes to join the cluster" in {
      joinCluster(node1, node2, node3)
    }

    "do something based on its state" in {
      clusterReplication.init(LockEntity())

      runOn(node1) {
        val entity  = clusterReplication.entityRefFor(LockEntity.typeKey, createSeqEntityId())
        val replyTo = actorTestKit.createTestProbe[LockEntity.State]()
        entity ! LockEntity.GetStatus(replyTo.ref)
        // initial state is UnLocking
        replyTo.receiveMessage() shouldBe a[LockEntity.UnLocking]
        // lock
        entity ! LockEntity.Lock()
        entity ! LockEntity.GetStatus(replyTo.ref)
        replyTo.receiveMessage() shouldBe a[LockEntity.Locking]
        // unlock
        entity ! LockEntity.UnLock()
        entity ! LockEntity.GetStatus(replyTo.ref)
        replyTo.receiveMessage() shouldBe a[LockEntity.UnLocking]
      }
    }

    "have state that is synchronized" in {
      clusterReplication.init(PingPongEntity())
      val probe = actorTestKit.createTestProbe[Receptionist.Listing]()

      runOn(node1) {
        val entity  = clusterReplication.entityRefFor(PingPongEntity.typeKey, createSeqEntityId())
        val replyTo = actorTestKit.createTestProbe[PingPongEntity.Pong]()

        entity ! PingPongEntity.Ping(replyTo.ref)
        replyTo.receiveMessage().count should be(1)
        entity ! PingPongEntity.Ping(replyTo.ref)
        replyTo.receiveMessage().count should be(2)
      }
      runOn(node1, node2, node3) {
        // find entity
        var actor: ActorRef[PingPongEntity.Command] = null
        awaitAssert {
          typedSystem.receptionist ! Receptionist.Subscribe(PingPongEntity.serviceKey, probe.ref)
          val listing = probe.receiveMessage()
          actor = listing.serviceInstances(PingPongEntity.serviceKey).head
          typedSystem.receptionist ! Receptionist.Deregister(PingPongEntity.serviceKey, actor)
        }
        // check state
        val stateReceiver = actorTestKit.createTestProbe[PingPongEntity.State]()
        awaitAssert {
          actor ! PingPongEntity.UnsafeGetState(stateReceiver.ref)
          stateReceiver.receiveMessage().count should be(2)
        }
      }
    }

    "be able to continue processing commands even if an exception occurred" in {
      clusterReplication.init(PingPongEntity())

      runOn(node1) {
        val entity  = clusterReplication.entityRefFor(PingPongEntity.typeKey, createSeqEntityId())
        val replyTo = actorTestKit.createTestProbe[PingPongEntity.Pong]()

        entity ! PingPongEntity.Ping(replyTo.ref)
        replyTo.receiveMessage().count should be(1)
        entity ! PingPongEntity.Ping(replyTo.ref)
        replyTo.receiveMessage().count should be(2)
        entity ! PingPongEntity.Break()
        awaitAssert {
          entity ! PingPongEntity.Ping(replyTo.ref)
          replyTo.receiveMessage().count should be(3)
        }
      }
    }

    "passivate all the entity replicas in the cluster" in {
      clusterReplication.init(EphemeralEntity())
      val entity = clusterReplication.entityRefFor(EphemeralEntity.typeKey, createSeqEntityId())
      val probe  = actorTestKit.createTestProbe[Receptionist.Listing]()

      runOn(node1) {
        entity ! EphemeralEntity.Start()
      }
      var actor: ActorRef[EphemeralEntity.Command] = null
      runOn(node1, node2, node3) {
        awaitAssert {
          typedSystem.receptionist ! Receptionist.Subscribe(EphemeralEntity.serviceKey, probe.ref)
          val listing = probe.receiveMessage()
          actor = listing.serviceInstances(EphemeralEntity.serviceKey).head
          typedSystem.receptionist ! Receptionist.Deregister(EphemeralEntity.serviceKey, actor)
        }
      }
      enterBarrier("entity member created")

      runOn(node1) {
        entity ! EphemeralEntity.Stop()
      }

      runOn(node1, node2, node3) {
        probe.expectTerminated(actor)
      }
    }

    "recover the entity state even if the entity was passivated" in {
      clusterReplication.init(EphemeralEntity())
      val entity  = clusterReplication.entityRefFor(EphemeralEntity.typeKey, createSeqEntityId())
      val replyTo = actorTestKit.createTestProbe[EphemeralEntity.State]()

      runOn(node1) {
        // initial value is 0
        entity ! EphemeralEntity.GetState(replyTo.ref)
        replyTo.receiveMessage().count should be(0)
        // increment the count
        entity ! EphemeralEntity.IncrementCount()
        entity ! EphemeralEntity.GetState(replyTo.ref)
        replyTo.receiveMessage().count should be(1)
      }
      var actor: ActorRef[EphemeralEntity.Command] = null
      runOn(node1, node2, node3) {
        val subscriber = actorTestKit.createTestProbe[Receptionist.Listing]()
        awaitAssert {
          typedSystem.receptionist ! Receptionist.Subscribe(EphemeralEntity.serviceKey, subscriber.ref)
          val listing = subscriber.receiveMessage()
          actor = listing.serviceInstances(EphemeralEntity.serviceKey).head
          typedSystem.receptionist ! Receptionist.Deregister(EphemeralEntity.serviceKey, actor)
        }
      }
      enterBarrier("entity members created")
      runOn(node1) {
        entity ! EphemeralEntity.Stop()
      }
      runOn(node1, node2, node3) {
        replyTo.expectTerminated(actor)
      }
      enterBarrier("entity members terminated")
      runOn(node1) {
        entity ! EphemeralEntity.GetState(replyTo.ref)
        replyTo.receiveMessage().count should be(1)
      }
    }
  }

  private[this] val idGenerator                 = new AtomicInteger(0)
  private[this] def createSeqEntityId(): String = s"replication-${idGenerator.incrementAndGet()}"
}

object ReplicatedEntityMultiNodeSpec {

  object PingPongEntity {
    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("PingPong")
    val serviceKey: ServiceKey[Command]           = ServiceKey[Command]("PingPongService")

    sealed trait Command                                      extends STMultiNodeSerializable
    final case class Ping(replyTo: ActorRef[Pong])            extends Command
    final case class Pong(count: Int)                         extends STMultiNodeSerializable
    final case class Break()                                  extends Command
    final case class UnsafeGetState(replyTo: ActorRef[State]) extends Command

    sealed trait Event         extends STMultiNodeSerializable
    final case class CountUp() extends Event

    final case class State(count: Int) extends STMultiNodeSerializable {

      def onMessage(message: Command): Effect[Event, State] =
        message match {
          case Ping(replyTo) =>
            Effect
              .replicate(CountUp())
              .thenReply(replyTo)(s => Pong(s.count))
          case Break() =>
            Effect
              .none[Event, State]
              .thenRun {
                throw new RuntimeException("bang!")
              }.thenNoReply()
          case UnsafeGetState(replyTo) =>
            replyTo ! this // unsafe! This is implemented for showing this entity status directly
            Effect.noReply
        }

      def applyEvent(event: Event): State =
        event match {
          case _ => copy(count = count + 1)
        }
    }

    def apply(): ReplicatedEntity[Command, ReplicationEnvelope[Command]] =
      ReplicatedEntity(typeKey)(entityContext =>
        Behaviors.setup { context =>
          context.system.receptionist ! Receptionist.Register(serviceKey, context.self)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(count = 0),
            commandHandler = _ onMessage _,
            eventHandler = _ applyEvent _,
          )
        },
      )
  }

  object LockEntity {
    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("Lock")

    sealed trait Command                                 extends STMultiNodeSerializable
    final case class Lock()                              extends Command
    final case class UnLock()                            extends Command
    final case class GetStatus(replyTo: ActorRef[State]) extends Command

    sealed trait Event          extends STMultiNodeSerializable
    final case class Locked()   extends Event
    final case class UnLocked() extends Event

    sealed trait State extends STMultiNodeSerializable {
      def onMessage(message: Command): Effect[Event, State]
      final def applyEvent(event: Event): State =
        event match {
          case Locked()   => Locking()
          case UnLocked() => UnLocking()
        }
    }

    final case class Locking() extends State {

      override def onMessage(message: Command): Effect[Event, State] =
        message match {
          case Lock() =>
            Effect.unhandled.thenNoReply()
          case UnLock() =>
            Effect.replicate(UnLocked()).thenNoReply()
          case GetStatus(replyTo) =>
            Effect.reply(replyTo)(this)
        }
    }

    final case class UnLocking() extends State {

      override def onMessage(message: Command): Effect[Event, State] =
        message match {
          case Lock() =>
            Effect.replicate(Locked()).thenNoReply()
          case UnLock() =>
            Effect.unhandled.thenNoReply()
          case GetStatus(replyTo) =>
            Effect.reply(replyTo)(this)
        }
    }

    def apply(): ReplicatedEntity[Command, ReplicationEnvelope[Command]] =
      ReplicatedEntity(typeKey)(context =>
        ReplicatedEntityBehavior[Command, Event, State](
          entityContext = context,
          emptyState = UnLocking(),
          commandHandler = _ onMessage _,
          eventHandler = _ applyEvent _,
        ),
      )
  }

  object EphemeralEntity {
    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("Ephemeral")
    val serviceKey: ServiceKey[Command]           = ServiceKey[Command]("EphemeralEntityService")

    sealed trait Command                                extends STMultiNodeSerializable
    final case class Start()                            extends Command
    final case class Stop()                             extends Command
    final case class IncrementCount()                   extends Command
    final case class GetState(replyTo: ActorRef[State]) extends Command

    sealed trait Event         extends STMultiNodeSerializable
    final case class CountUp() extends Event

    final case class State(count: Int) extends STMultiNodeSerializable {

      def onMessage(message: Command): Effect[Event, State] =
        message match {
          case Start() =>
            Effect.noReply
          case Stop() =>
            Effect.passivate().thenNoReply()
          case IncrementCount() =>
            Effect.replicate(CountUp()).thenNoReply()
          case GetState(replyTo) =>
            Effect.reply(replyTo)(this)
        }

      def applyEvent(event: Event): State =
        event match {
          case CountUp() => copy(count = count + 1)
        }
    }

    def apply(): ReplicatedEntity[Command, ReplicationEnvelope[Command]] =
      ReplicatedEntity(typeKey)(entityContext =>
        Behaviors.setup { context =>
          context.system.receptionist ! Receptionist.Register(serviceKey, context.self)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(count = 0),
            commandHandler = _ onMessage _,
            eventHandler = _ applyEvent _,
          )
        },
      )
  }
}
