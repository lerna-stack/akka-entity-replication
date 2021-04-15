package lerna.akka.entityreplication.typed

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }

/**
  * verify type definition: This code doesn't make sense in context
  */
object ReplicatedEntityBehaviorSpec {

  import akka.actor.typed.ActorRef
  import akka.actor.typed.Behavior

  object BankAccountBehavior {
    sealed trait Command
    final case object WakeUp                                                 extends Command
    final case object Stop                                                   extends Command
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
              .thenPassivate()
              .thenUnstashAll()
              .thenReply(replyTo)(s => DepositSuccess(amount, s.balance))

          case Withdraw(amount, replyTo) =>
            if (canWithdraw(amount))
              Effect
                .replicate(Withdrawn(amount))
                .thenRun { _: State =>
                  context.log.info("complete withdrawing")
                }
                .thenPassivate()
                .thenReply(replyTo)(s => WithdrawSuccess(amount, s.balance))
            else
              Effect.reply(replyTo)(ShortBalance)

          case GetBalance(replyTo) =>
            Effect.reply(replyTo)(AccountBalance(balance))

          case WakeUp if balance > 0 =>
            Effect
              .unstashAll()
              .thenRun { _: State =>
                context.log.warn("unstashAll")
              }.thenNoReply()

          case WakeUp =>
            Effect.noReply

          case Stop =>
            Effect.passivate().thenNoReply()

          case command =>
            Effect.unhandled
              .thenRun { _: State =>
                context.log.warn(s"unhandled ${command}")
              }
              .thenNoReply()
        }

      override def applyEvent(event: Event, context: ActorContext[Command]): State =
        event match {
          case Deposited(amount) => deposit(amount)
          case Withdrawn(amount) => withdraw(amount)
        }
    }

    def apply(entityContext: ReplicatedEntityContext[Command]): Behavior[Command] = {
      Behaviors.setup { context =>
        ReplicatedEntityBehavior[Command, Event, State](
          entityContext,
          emptyState = Account(balance = 0),
          commandHandler = (state, command) => state.applyCommand(command, context),
          eventHandler = (state, event) => state.applyEvent(event, context),
        )
      }
    }
  }

  def startRegion(system: ActorSystem[_]): ActorRef[ReplicationEnvelope[BankAccountBehavior.Command]] = {
    val typeKey = ReplicatedEntityTypeKey[BankAccountBehavior.Command]("test")
    ClusterReplication(system).init(ReplicatedEntity(typeKey) { entityContext =>
      BankAccountBehavior(entityContext)
    })
  }
}
