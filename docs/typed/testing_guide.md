# Testing Guide

## Getting started

TestKit that is described by this doc requires following module.
Please add the dependency to your project.

```scala
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test
)
```

## ReplicatedEntityBehaviorTestKit

To unit test `ReplicatedEntityBehavior`, you can use `ReplicatedEntityBehaviorTestKit`.
It makes the behavior run a command, and you can assert the returned result is as expected.
The result contains the following elements:

- The event emitted by the command
- New state after applying the events
- Reply message to a command (if the command provides it)

The TestKit verifies serialization of commands, events and state automatically.

To test restoring the behavior, you can use `ReplicatedEntityBehaviorTestKit.restart()` method.
It will restart the behavior, and then recover the state from stored snapshot and events from commands that are processed.

Following example is a test for the `BankAccountBehavior` which is shown in the [Implementation Guide](./implementation_guide.md):

```scala
class BankAccountBehaviorSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  import BankAccountBehavior._

  private[this] val testKit = ActorTestKit()

  private[this] val bankAccountTestKit =
    ReplicatedEntityBehaviorTestKit[Command, DomainEvent, Account](
      testKit.system,
      BankAccountBehavior.TypeKey,
      entityId = "test-entity",
      behavior = context => BankAccountBehavior(context),
    )

  override def afterEach(): Unit = {
    bankAccountTestKit.clear()
    super.afterEach()
  }

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "A BankAccountBehavior" should {

    "increase a balance when it receives Deposit" in {
      val transactionId1 = 1L
      val result1        = bankAccountTestKit.runCommand(Deposit(transactionId1, amount = 1000, _))
      result1.eventOfType[Deposited].amount should be(1000)
      result1.state.balance should be(1000)
      result1.reply.balance should be(1000)

      val transactionId2 = 2L
      val result2        = bankAccountTestKit.runCommand(Deposit(transactionId2, amount = 2000, _))
      result2.eventOfType[Deposited].amount should be(2000)
      result2.state.balance should be(3000)
      result2.reply.balance should be(3000)
    }
    
    "restore the balance after it restarts" in {
      val result1 = bankAccountTestKit.runCommand(Deposit(transactionId = 1L, amount = 1000, _))
      result1.reply.balance should be(1000)
      val result2 = bankAccountTestKit.runCommand(Withdraw(transactionId = 2L, amount = 500, _))
      result2.replyOfType[WithdrawSucceeded].balance should be(500)

      bankAccountTestKit.restart()
      bankAccountTestKit.state.balance should be(500)
    }
  }
}
```
