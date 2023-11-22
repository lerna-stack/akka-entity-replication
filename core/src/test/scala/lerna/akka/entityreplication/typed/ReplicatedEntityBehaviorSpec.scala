package lerna.akka.entityreplication.typed

import akka.actor.UnhandledMessage
import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, TestProbe }
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior, PostStop }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.typed.ClusterReplication.ShardCommand
import org.scalatest.{ BeforeAndAfterAll, Inside, Matchers, WordSpec }
import akka.actor.typed.scaladsl.adapter._
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol

import java.util.concurrent.atomic.AtomicInteger

object ReplicatedEntityBehaviorSpec {

  import akka.actor.typed.ActorRef
  import akka.actor.typed.Behavior

  object BankAccountBehavior {

    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("BankAccount")

    sealed trait Command
    final case object Passivate                                              extends Command
    final case object Stop                                                   extends Command
    final case object SimulateFailure                                        extends Command
    final case object Unhandled                                              extends Command
    final case object Lock                                                   extends Command
    final case object Unlock                                                 extends Command
    final case class Deposit(amount: Int, replyTo: ActorRef[DepositReply])   extends Command
    final case class Withdraw(amount: Int, replyTo: ActorRef[WithdrawReply]) extends Command
    final case class GetBalance(replyTo: ActorRef[AccountBalance])           extends Command

    trait Reply
    sealed trait DepositReply                                   extends Reply
    final case class DepositSuccess(amount: Int, balance: Int)  extends DepositReply
    sealed trait WithdrawReply                                  extends Reply
    final case object ShortBalance                              extends WithdrawReply
    final case class WithdrawSuccess(amount: Int, balance: Int) extends WithdrawReply
    sealed trait GetBalanceReply                                extends Reply
    final case class AccountBalance(balance: Int)               extends GetBalanceReply

    sealed trait Event
    final case class Deposited(amount: Int) extends Event
    final case class Withdrawn(amount: Int) extends Event
    final case class Locked()               extends Event
    final case class Unlocked()             extends Event

    final case object PostStopReceived

    type Effect = lerna.akka.entityreplication.typed.Effect[Event, State]

    sealed trait State {
      def balance: Int
      def applyCommand(command: Command, context: ActorContext[Command]): Effect
      def applyEvent(event: Event, context: ActorContext[Command]): State
    }
    final case class Account(balance: Int) extends State {
      def deposit(amount: Int): Account     = copy(balance = balance + amount)
      def withdraw(amount: Int): Account    = copy(balance = balance - amount)
      def canWithdraw(amount: Int): Boolean = balance - amount >= 0

      override def applyCommand(command: Command, context: ActorContext[Command]): Effect =
        command match {

          case Deposit(amount, replyTo) =>
            Effect
              .replicate(Deposited(amount))
              .thenRun { _: State =>
                context.log.info("complete depositing")
              }
              .thenReply(replyTo)(s => DepositSuccess(amount, s.balance))

          case Withdraw(amount, replyTo) =>
            if (canWithdraw(amount))
              Effect
                .replicate(Withdrawn(amount))
                .thenRun { _: State =>
                  context.log.info("complete withdrawing")
                }
                .thenReply(replyTo)(s => WithdrawSuccess(amount, s.balance))
            else
              Effect.reply(replyTo)(ShortBalance)

          case GetBalance(replyTo) =>
            Effect.reply(replyTo)(AccountBalance(balance))

          case Passivate =>
            Effect.none.thenPassivate().thenNoReply()

          case Stop =>
            Effect.stopLocally()

          case SimulateFailure =>
            throw new IllegalStateException("bang!")

          case Unhandled =>
            Effect.unhandled.thenNoReply()

          case Lock =>
            Effect.replicate(Locked()).thenNoReply()

          case Unlock =>
            Effect.noReply
        }

      override def applyEvent(event: Event, context: ActorContext[Command]): State =
        event match {
          case Deposited(amount) => deposit(amount)
          case Withdrawn(amount) => withdraw(amount)
          case Locked()          => Locking(balance)
          case Unlocked()        => this
        }
    }

    final case class Locking(balance: Int) extends State {
      override def applyCommand(command: Command, context: ActorContext[Command]): Effect =
        command match {
          case Unlock =>
            Effect.replicate(Unlocked()).thenUnstashAll().thenNoReply()
          case _ =>
            Effect.stash()
        }

      override def applyEvent(event: Event, context: ActorContext[Command]): State =
        event match {
          case Deposited(_) => throw new UnsupportedOperationException()
          case Withdrawn(_) => throw new UnsupportedOperationException()
          case Locked()     => this
          case Unlocked()   => Account(balance)
        }
    }

    def apply(entityContext: ReplicatedEntityContext[Command]): Behavior[Command] = {
      Behaviors.setup { context =>
        ReplicatedEntityBehavior[Command, Event, State](
          entityContext,
          emptyState = Account(balance = 0),
          commandHandler = (state, command) => state.applyCommand(command, context),
          eventHandler = (state, event) => state.applyEvent(event, context),
        ).withStopMessage(Stop).receiveSignal {
          case (_, PostStop) =>
            context.system.eventStream ! EventStream.Publish(PostStopReceived)
        }
      }
    }
  }

  def startRegion(system: ActorSystem[_]): ActorRef[ReplicationEnvelope[BankAccountBehavior.Command]] = {
    ClusterReplication(system).init(ReplicatedEntity(BankAccountBehavior.typeKey) { entityContext =>
      BankAccountBehavior(entityContext)
    })
  }
}

class ReplicatedEntityBehaviorSpec extends WordSpec with BeforeAndAfterAll with Matchers with Inside {
  import ReplicatedEntityBehaviorSpec._

  private[this] val testkit = ActorTestKit()

  override def afterAll(): Unit = testkit.shutdownTestKit()

  private[this] val shardProbe   = testkit.createTestProbe[ShardCommand]()
  private val snapshotStoreProbe = testkit.createTestProbe[SnapshotProtocol.Command]()

  private[this] val entityId           = "entity1"
  private[this] val normalizedEntityId = NormalizedEntityId.from(entityId)
  private[this] val entityContext: ReplicatedEntityContext[BankAccountBehavior.Command] =
    new ReplicatedEntityContext(
      BankAccountBehavior.typeKey,
      entityId = entityId,
      shard = shardProbe.ref,
    )

  private[this] implicit class EntityHelper(actorRef: ActorRef[BankAccountBehavior.Command]) {
    def asEntity: ActorRef[RaftProtocol.EntityCommand] = actorRef.unsafeUpcast

    def askWithTestProbe[Reply](f: ActorRef[Reply] => BankAccountBehavior.Command): TestProbe[Reply] = {
      val testProbe = testkit.createTestProbe[Reply]()
      actorRef ! f(testProbe.ref)
      testProbe
    }
  }

  private[this] val logEntryIndexGenerator = new AtomicInteger(1)
  private[this] def nextLogEntryIndex()    = LogEntryIndex(logEntryIndexGenerator.getAndIncrement())

  "ReplicatedEntityBehavior" when {

    "Inactive" should {

      "stash ProcessCommand until recovery completed" in {
        val bankAccount = testkit.spawn(BankAccountBehavior(entityContext))

        // The entity will stash a command.
        val depositReplyProbe = bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(10, _))
        // Assert: The entity doesn't reply to the command before it completes a recovery.
        depositReplyProbe.expectNoMessage()

        // Activate the entity.
        val lastApplied = LogEntryIndex(6)
        bankAccount.asEntity ! RaftProtocol.Activate(snapshotStoreProbe.ref.toClassic, recoveryIndex = lastApplied)

        // Assert: The entity doesn't reply to the command before it completes the recovery.
        depositReplyProbe.expectNoMessage()

        // Recover the entity.
        locally {
          val metadata = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, lastApplied)
          val state    = SnapshotProtocol.EntityState(BankAccountBehavior.Account(100))
          recoverWithState(bankAccount.asEntity, SnapshotProtocol.EntitySnapshot(metadata, state))
        }

        // The entity will unstash and handle the command after it completes the recovery.
        locally {
          val replicate = shardProbe.expectMessageType[RaftProtocol.Replicate]
          replicate.replyTo ! RaftProtocol.ReplicationSucceeded(replicate.event, LogEntryIndex(7), replicate.instanceId)
          depositReplyProbe.expectMessageType[BankAccountBehavior.DepositSuccess].balance should be(110)
        }

        testkit.stop(bankAccount)
      }

      "stash TakeSnapshot until recovery completed" in {
        val bankAccount = testkit.spawn(BankAccountBehavior(entityContext))

        val snapshotMetadata    = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, LogEntryIndex(5))
        val lastApplied         = LogEntryIndex(6)
        val newSnapshotMetadata = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, lastApplied)

        // The entity will stash a TakeSnapshot.
        val takeSnapshotReplyProbe = testkit.createTestProbe[RaftProtocol.Snapshot]()
        bankAccount.asEntity ! RaftProtocol.TakeSnapshot(newSnapshotMetadata, takeSnapshotReplyProbe.ref.toClassic)
        // Assert: The entity doesn't reply to the TakeSnapshot before it completes a recovery.
        takeSnapshotReplyProbe.expectNoMessage()

        // Activate the entity.
        bankAccount.asEntity ! RaftProtocol.Activate(snapshotStoreProbe.ref.toClassic, recoveryIndex = lastApplied)

        // Assert: The entity doesn't reply to the TakeSnapshot before it completes the recovery.
        takeSnapshotReplyProbe.expectNoMessage()

        // Recover the entity.
        val state = SnapshotProtocol.EntityState(BankAccountBehavior.Account(100))
        recoverWithState(bankAccount.asEntity, SnapshotProtocol.EntitySnapshot(snapshotMetadata, state))

        // The entity will unstash and handle the TakeSnapshot after it completes the recovery.
        takeSnapshotReplyProbe.expectMessage(RaftProtocol.Snapshot(newSnapshotMetadata, state))

        testkit.stop(bankAccount)
      }

      "stash Replica until recovery completed" in {
        val bankAccount = testkit.spawn(BankAccountBehavior(entityContext))

        // The entity has a balance of 100. It will deposit 10 to the balance.
        val bankAccountState    = BankAccountBehavior.Account(100)
        val bankAccountNewEvent = BankAccountBehavior.Deposited(10)

        val lastApplied = LogEntryIndex(5)

        // The entity will stash a Replica.
        bankAccount.asEntity ! RaftProtocol.Replica(
          LogEntry(LogEntryIndex(6), EntityEvent(Option(normalizedEntityId), bankAccountNewEvent), Term(2)),
        )

        // Activate the entity.
        bankAccount.asEntity ! RaftProtocol.Activate(snapshotStoreProbe.ref.toClassic, recoveryIndex = lastApplied)

        // Recover the entity.
        locally {
          val metadata = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, lastApplied)
          val state    = SnapshotProtocol.EntityState(bankAccountState)
          recoverWithState(bankAccount.asEntity, SnapshotProtocol.EntitySnapshot(metadata, state))
        }

        // The entity will unstash and handle the Replica after it completes the recovery.
        // The entity should have a new balance of 110 (= 100 + 10).
        locally {
          val getBalanceReplyProbe = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
          val replicate            = shardProbe.expectMessageType[RaftProtocol.Replicate]
          replicate.replyTo ! RaftProtocol.ReplicationSucceeded(replicate.event, LogEntryIndex(7), replicate.instanceId)
          getBalanceReplyProbe.expectMessage(BankAccountBehavior.AccountBalance(110))
        }

        testkit.stop(bankAccount)
      }

    }

  }

  "ReplicatedEntityBehavior" should {

    "process command that updates the entity state" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      inside(snapshotStoreProbe.receiveMessage()) {
        case SnapshotProtocol.FetchSnapshot(entityId, replyTo) =>
          entityId should be(normalizedEntityId)
          replyTo ! SnapshotProtocol.SnapshotNotFound(entityId)
      }
      inside(shardProbe.receiveMessage()) {
        case FetchEntityEvents(entityId, from, to, replyTo) =>
          entityId should be(normalizedEntityId)
          from should be(LogEntryIndex.initial().next())
          to should be(LogEntryIndex.initial())
          replyTo ! FetchEntityEventsResponse(Seq())
      }

      // process a command with replicate
      val replyProbe = bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(100, _))
      val replicate =
        inside(shardProbe.expectMessageType[RaftProtocol.Replicate]) {
          case cmd @ RaftProtocol.Replicate.ReplicateForEntity(
                event,
                replyTo,
                entityId,
                _,
                entityLastAppliedIndex,
                originSender,
              ) =>
            event shouldBe a[BankAccountBehavior.Deposited]
            replyTo should be(bankAccount.toClassic)
            entityId should be(normalizedEntityId)
            entityLastAppliedIndex should be(LogEntryIndex(0))
            originSender should be(testkit.system.deadLetters.toClassic)
            cmd
        }
      replicate.replyTo ! RaftProtocol.ReplicationSucceeded(replicate.event, nextLogEntryIndex(), replicate.instanceId)

      // get reply
      inside(replyProbe.expectMessageType[BankAccountBehavior.DepositSuccess]) {
        case BankAccountBehavior.DepositSuccess(amount, balance) =>
          amount should be(100)
          balance should be(100)
      }

      testkit.stop(bankAccount)
    }

    "process command that only reads state" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      import SnapshotProtocol._
      val metadata = EntitySnapshotMetadata(normalizedEntityId, nextLogEntryIndex())
      val state    = EntityState(BankAccountBehavior.Account(100))
      recoverWithState(bankAccount.asEntity, EntitySnapshot(metadata, state))

      // process a command with ensuring consistency
      val replyProbe = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      val replicate =
        inside(shardProbe.expectMessageType[RaftProtocol.Replicate]) {
          case cmd @ RaftProtocol.Replicate.ReplicateForEntity(
                event,
                replyTo,
                entityId,
                _,
                entityLastAppliedIndex,
                originSender,
              ) =>
            event should be(NoOp)
            replyTo should be(bankAccount.toClassic)
            entityId should be(normalizedEntityId)
            entityLastAppliedIndex should be(metadata.logEntryIndex)
            originSender should be(testkit.system.deadLetters.toClassic)
            cmd
        }
      replicate.replyTo ! RaftProtocol.ReplicationSucceeded(replicate.event, nextLogEntryIndex(), replicate.instanceId)

      // get reply
      inside(replyProbe.expectMessageType[BankAccountBehavior.AccountBalance]) {
        case BankAccountBehavior.AccountBalance(balance) =>
          balance should be(100)
      }

      testkit.stop(bankAccount)
    }

    "recovery state with the snapshot state and replicated events that is send from the shard" in {
      import SnapshotProtocol._
      val metadata    = EntitySnapshotMetadata(normalizedEntityId, logEntryIndex = LogEntryIndex(5))
      val state       = EntityState(BankAccountBehavior.Account(100))
      val term        = Term(1)
      val lastApplied = LogEntryIndex(8)
      val events = Seq(
        LogEntry(LogEntryIndex(6), EntityEvent(Option(normalizedEntityId), BankAccountBehavior.Deposited(10)), term),
        LogEntry(LogEntryIndex(7), EntityEvent(Option(normalizedEntityId), BankAccountBehavior.Deposited(10)), term),
        LogEntry(LogEntryIndex(8), EntityEvent(Option(normalizedEntityId), BankAccountBehavior.Deposited(10)), term),
      )

      val bankAccount = testkit.spawn(BankAccountBehavior(entityContext))
      bankAccount.asEntity ! RaftProtocol.Activate(snapshotStoreProbe.ref.toClassic, recoveryIndex = lastApplied)

      // recover the entity
      inside(snapshotStoreProbe.receiveMessage()) {
        case SnapshotProtocol.FetchSnapshot(_, replyTo) =>
          replyTo ! SnapshotProtocol.SnapshotFound(EntitySnapshot(metadata, state))
      }
      inside(shardProbe.receiveMessage()) {
        case FetchEntityEvents(_, from, to, replyTo) =>
          from should be(metadata.logEntryIndex.next())
          to should be(lastApplied)
          replyTo ! FetchEntityEventsResponse(events)
      }

      val replyProbe = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)

      // ensure consistency
      val replicate = shardProbe.expectMessageType[RaftProtocol.Replicate]
      replicate.replyTo ! RaftProtocol.ReplicationSucceeded(replicate.event, LogEntryIndex(9), replicate.instanceId)

      // get reply
      replyProbe.expectMessageType[BankAccountBehavior.AccountBalance].balance should be(130)

      testkit.stop(bankAccount)
    }

    "send a passivation command to shard when receiving the stop message" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      inside(snapshotStoreProbe.receiveMessage()) {
        case SnapshotProtocol.FetchSnapshot(entityId, replyTo) =>
          replyTo ! SnapshotProtocol.SnapshotNotFound(entityId)
      }
      inside(shardProbe.receiveMessage()) {
        case FetchEntityEvents(_, _, _, replyTo) =>
          replyTo ! FetchEntityEventsResponse(Seq())
      }

      bankAccount ! BankAccountBehavior.Passivate

      // shard receives passivate message
      inside(shardProbe.expectMessageType[ReplicationRegion.Passivate]) {
        case ReplicationRegion.Passivate(entityPath, stopMessage) =>
          entityPath should be(bankAccount.path)
          stopMessage should be(BankAccountBehavior.Stop)
      }
      bankAccount ! BankAccountBehavior.Stop

      shardProbe.expectTerminated(bankAccount)
    }

    "stash command until recovery completed" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // the command will be stash
      val replyToProbe = bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(10, _))
      replyToProbe.expectNoMessage()

      // recover the entity
      import SnapshotProtocol._
      val metadata = EntitySnapshotMetadata(normalizedEntityId, nextLogEntryIndex())
      val state    = EntityState(BankAccountBehavior.Account(100))
      recoverWithState(bankAccount.asEntity, EntitySnapshot(metadata, state))

      // unstash the command when recovery completed
      val replicate = shardProbe.expectMessageType[RaftProtocol.Replicate]
      replicate.replyTo ! RaftProtocol.ReplicationSucceeded(replicate.event, nextLogEntryIndex(), replicate.instanceId)

      // get reply
      replyToProbe.expectMessageType[BankAccountBehavior.DepositSuccess].balance should be(110)

      testkit.stop(bankAccount)
    }

    "stash TakeSnapshot until recovery completed" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      val snapshotMetadata    = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, LogEntryIndex(5))
      val newSnapshotMetadata = SnapshotProtocol.EntitySnapshotMetadata(normalizedEntityId, LogEntryIndex(6))

      // The entity will stash a TakeSnapshot.
      val takeSnapshotReplyProbe = testkit.createTestProbe[RaftProtocol.Snapshot]()
      bankAccount.asEntity ! RaftProtocol.TakeSnapshot(newSnapshotMetadata, takeSnapshotReplyProbe.ref.toClassic)
      // Assert: The entity doesn't reply to the TakeSnapshot before it completes a recovery.
      takeSnapshotReplyProbe.expectNoMessage()

      // Recover the entity.
      val state = SnapshotProtocol.EntityState(BankAccountBehavior.Account(100))
      recoverWithState(bankAccount.asEntity, SnapshotProtocol.EntitySnapshot(snapshotMetadata, state))

      // The entity will unstash and handle the TakeSnapshot after it completes the recovery.
      takeSnapshotReplyProbe.expectMessage(RaftProtocol.Snapshot(newSnapshotMetadata, state))

      testkit.stop(bankAccount)
    }

    "stash command until replication completed" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      import SnapshotProtocol._
      val metadata = EntitySnapshotMetadata(normalizedEntityId, nextLogEntryIndex())
      val state    = EntityState(BankAccountBehavior.Account(100))
      recoverWithState(bankAccount.asEntity, EntitySnapshot(metadata, state))

      val getBalanceProbe = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)

      // the command will be stash
      val depositReplyProbe = bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(10, _))

      // unstash the command when replication completed
      val r1 = shardProbe.expectMessageType[RaftProtocol.Replicate]
      shardProbe.expectNoMessage() // because Deposit command is stashed
      r1.replyTo ! RaftProtocol.ReplicationSucceeded(r1.event, nextLogEntryIndex(), r1.instanceId)

      getBalanceProbe.expectMessageType[BankAccountBehavior.AccountBalance].balance should be(100)

      val r2 = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r2.replyTo ! RaftProtocol.ReplicationSucceeded(r2.event, nextLogEntryIndex(), r2.instanceId)

      // get reply
      depositReplyProbe.expectMessageType[BankAccountBehavior.DepositSuccess].balance should be(110)

      testkit.stop(bankAccount)
    }

    "transitions to `Ready` state and processes the next command without executing any side effect if ReplicationActor receives ReplicationFailed in `WaitForReplication` state" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      {
        // recover the entity
        import SnapshotProtocol._
        val metadata = EntitySnapshotMetadata(normalizedEntityId, nextLogEntryIndex())
        val state    = EntityState(BankAccountBehavior.Account(100))
        recoverWithState(bankAccount.asEntity, EntitySnapshot(metadata, state))
      }

      val clientProbe = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      shardProbe.expectMessageType[RaftProtocol.Replicate]
      clientProbe.expectNoMessage()

      bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      shardProbe.expectNoMessage() // the command is stashed

      bankAccount.asEntity ! RaftProtocol.ReplicationFailed // from raftActor
      clientProbe.expectNoMessage()
      shardProbe.expectMessageType[RaftProtocol.Replicate]

      testkit.stop(bankAccount)
    }

    "can process commands after receiving Replica even if replication is in progress" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)

      // process a command
      val depositReplyProbe1 = bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(100, _))
      shardProbe.expectMessageType[RaftProtocol.Replicate]
      // replication is interrupted by receiving replicated log entry
      val replicatedLogEntry =
        LogEntry(
          nextLogEntryIndex(),
          EntityEvent(Option(normalizedEntityId), BankAccountBehavior.Deposited(10)),
          Term(1),
        )
      bankAccount.asEntity ! RaftProtocol.Replica(replicatedLogEntry)
      depositReplyProbe1.expectNoMessage()

      // process another command
      val depositReplyProbe2 = bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(100, _))
      val r                  = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r.replyTo ! RaftProtocol.ReplicationSucceeded(r.event, nextLogEntryIndex(), r.instanceId)

      // get reply
      depositReplyProbe2.expectMessageType[BankAccountBehavior.DepositSuccess].balance should be(110)

      testkit.stop(bankAccount)
    }

    "replace instanceId when it restarted" in {
      var bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)

      // process a command
      bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      val r1 = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r1.replyTo ! RaftProtocol.ReplicationSucceeded(r1.event, nextLogEntryIndex(), r1.instanceId)

      // the command cause an error
      bankAccount ! BankAccountBehavior.SimulateFailure
      // recover the entity
      bankAccount = spawnEntity(BankAccountBehavior(entityContext))
      recoverWithInitialState(bankAccount.asEntity)

      // process a command
      bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      val r2 = shardProbe.expectMessageType[RaftProtocol.Replicate]

      r1.instanceId should not be r2.instanceId

      testkit.stop(bankAccount)
    }

    "ignore ReplicationSucceeded which has old instanceId" in {
      var bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)

      // process a command
      bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      val r1               = shardProbe.expectMessageType[RaftProtocol.Replicate]
      val replicationReply = RaftProtocol.ReplicationSucceeded(r1.event, nextLogEntryIndex(), r1.instanceId)
      r1.replyTo ! replicationReply

      // the command cause an error
      bankAccount ! BankAccountBehavior.SimulateFailure
      // recover the entity
      bankAccount = spawnEntity(BankAccountBehavior(entityContext))
      recoverWithInitialState(bankAccount.asEntity)

      // process a command
      val getBalance = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      val r2         = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r2.replyTo ! replicationReply // the reply has old instanceId (will be ignore)
      getBalance.expectNoMessage()
      r2.replyTo ! RaftProtocol.ReplicationSucceeded(r2.event, nextLogEntryIndex(), r2.instanceId)
      getBalance.expectMessageType[BankAccountBehavior.AccountBalance]

      testkit.stop(bankAccount)
    }

    "ignore Replica that has older LogEntryIndex than already applied ones" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)

      val replicatedLogEntry =
        LogEntry(
          nextLogEntryIndex(),
          EntityEvent(Option(normalizedEntityId), BankAccountBehavior.Deposited(10)),
          Term(1),
        )
      // send twice
      bankAccount.asEntity ! RaftProtocol.Replica(replicatedLogEntry)
      bankAccount.asEntity ! RaftProtocol.Replica(replicatedLogEntry)

      val getBalance = bankAccount.askWithTestProbe(BankAccountBehavior.GetBalance)
      val r          = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r.replyTo ! RaftProtocol.ReplicationSucceeded(r.event, nextLogEntryIndex(), r.instanceId)

      // Replica with the same LogEntryIndex should not be applied in duplicate
      getBalance.expectMessageType[BankAccountBehavior.AccountBalance].balance should be(10)

      testkit.stop(bankAccount)
    }

    "stop when illegal any settings exist" in {
      val config = ConfigFactory
        .parseString("lerna.akka.entityreplication.raft.number-of-shards = 0")
        .withFallback(this.testkit.config)

      val localTestkit = ActorTestKit(config)
      val bankAccount  = spawnEntity(BankAccountBehavior(entityContext), localTestkit)

      shardProbe.expectTerminated(bankAccount)

      localTestkit.stop(bankAccount)
      localTestkit.shutdownTestKit()
    }

    "reboot and retry recovery after recovery-entity-timeout" in {
      val config = ConfigFactory
        .parseString("lerna.akka.entityreplication.recovery-entity-timeout = 0.5s")
        .withFallback(this.testkit.config)
      val localTestkit = ActorTestKit(config)
      var bankAccount  = spawnEntity(BankAccountBehavior(entityContext), localTestkit)

      snapshotStoreProbe.expectMessageType[SnapshotProtocol.FetchSnapshot]
      // timeout after 0.5s
      shardProbe.expectTerminated(bankAccount)
      bankAccount = spawnEntity(BankAccountBehavior(entityContext), localTestkit)
      inside(snapshotStoreProbe.expectMessageType[SnapshotProtocol.Command]) {
        case SnapshotProtocol.FetchSnapshot(entityId, replyTo) =>
          replyTo ! SnapshotProtocol.SnapshotNotFound(entityId)
      }
      shardProbe.expectMessageType[FetchEntityEvents]
      // timeout after 0.5s
      shardProbe.expectTerminated(bankAccount)
      bankAccount = spawnEntity(BankAccountBehavior(entityContext), localTestkit)
      snapshotStoreProbe.expectMessageType[SnapshotProtocol.FetchSnapshot]

      localTestkit.stop(bankAccount)
      localTestkit.shutdownTestKit()
    }

    "handle signal" in {
      val bankAccount           = spawnEntity(BankAccountBehavior(entityContext))
      val eventStreamSubscriber = testkit.createTestProbe[BankAccountBehavior.PostStopReceived.type]()

      testkit.system.eventStream ! EventStream.Subscribe(eventStreamSubscriber.ref)

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)
      bankAccount ! BankAccountBehavior.Stop

      // PostStopReceived is published to EventStream by PostStop signal
      eventStreamSubscriber.receiveMessage() should be(BankAccountBehavior.PostStopReceived)

      testkit.system.eventStream ! EventStream.Unsubscribe(eventStreamSubscriber.ref)

      testkit.stop(bankAccount)
    }

    "publish unhandled command to Akka event stream" in {
      val bankAccount           = spawnEntity(BankAccountBehavior(entityContext))
      val eventStreamSubscriber = testkit.createTestProbe[UnhandledMessage]()

      testkit.system.eventStream ! EventStream.Subscribe(eventStreamSubscriber.ref)

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)

      bankAccount ! BankAccountBehavior.Unhandled

      inside(eventStreamSubscriber.receiveMessage()) {
        case UnhandledMessage(message, sender, recipient) =>
          message should be(BankAccountBehavior.Unhandled)
          sender should be(testkit.system.deadLetters.toClassic)
          recipient should be(bankAccount.toClassic)
      }

      testkit.system.eventStream ! EventStream.Unsubscribe(eventStreamSubscriber.ref)
      testkit.stop(bankAccount)
    }

    "stash a user defined command when stash effect is used" in {
      val bankAccount = spawnEntity(BankAccountBehavior(entityContext))

      // recover the entity
      recoverWithInitialState(bankAccount.asEntity)

      bankAccount ! BankAccountBehavior.Lock
      val r1 = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r1.event shouldBe a[BankAccountBehavior.Locked]
      r1.replyTo ! RaftProtocol.ReplicationSucceeded(r1.event, nextLogEntryIndex(), r1.instanceId)

      val replyProbe: TestProbe[BankAccountBehavior.DepositReply] =
        bankAccount.askWithTestProbe(BankAccountBehavior.Deposit(1000, _)) // this command will be stashed
      shardProbe.expectNoMessage()
      replyProbe.expectNoMessage()

      bankAccount ! BankAccountBehavior.Unlock // induces unstashAll
      val r2 = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r2.event shouldBe a[BankAccountBehavior.Unlocked]
      r2.replyTo ! RaftProtocol.ReplicationSucceeded(r2.event, nextLogEntryIndex(), r2.instanceId)

      val r3 = shardProbe.expectMessageType[RaftProtocol.Replicate]
      r3.event shouldBe a[BankAccountBehavior.Deposited]
      r3.replyTo ! RaftProtocol.ReplicationSucceeded(r3.event, nextLogEntryIndex(), r3.instanceId)
      replyProbe.receiveMessage()

      testkit.stop(bankAccount)
    }
  }

  private def spawnEntity(
      behavior: Behavior[BankAccountBehavior.Command],
      testkit: ActorTestKit = testkit,
  ): ActorRef[BankAccountBehavior.Command] = {
    val bankAccount = testkit.spawn(behavior)
    bankAccount.asEntity ! RaftProtocol.Activate(snapshotStoreProbe.ref.toClassic, LogEntryIndex.initial())
    bankAccount
  }

  private def recoverWithInitialState(entity: ActorRef[RaftProtocol.EntityCommand]): Unit = {
    inside(snapshotStoreProbe.receiveMessage()) {
      case SnapshotProtocol.FetchSnapshot(entityId, replyTo) =>
        replyTo ! SnapshotProtocol.SnapshotNotFound(entityId)
    }
    inside(shardProbe.receiveMessage()) {
      case FetchEntityEvents(_, _, _, replyTo) =>
        replyTo ! FetchEntityEventsResponse(Seq())
    }
  }

  private def recoverWithState(
      entity: ActorRef[RaftProtocol.EntityCommand],
      entitySnapshot: SnapshotProtocol.EntitySnapshot,
  ): Unit = {
    val entityId = entitySnapshot.metadata.entityId
    inside(snapshotStoreProbe.receiveMessage()) {
      case SnapshotProtocol.FetchSnapshot(`entityId`, replyTo) =>
        replyTo ! SnapshotProtocol.SnapshotFound(entitySnapshot)
    }
    inside(shardProbe.receiveMessage()) {
      case FetchEntityEvents(_, _, _, replyTo) =>
        replyTo ! FetchEntityEventsResponse(Seq())
    }
  }

}
