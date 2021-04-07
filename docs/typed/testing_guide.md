# Testing Guide

## ReplicatedEntityBehaviorTestKit

```scala
class BankAccountBehaviorSpec 
  extends TestKit(ActorSystem("BankAccountBehaviorSpec")) 
    with WordSpecLike 
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  private[this] val replicatedEntityTestkit =
    ReplicatedEntityBehaviorTestKit[BankAccountBehavior.Command, BankAccountBehavior.Event, BankAccountBehavior.State] (
      system,
      BankAccountBehavior(ReplicationId("Account", "1"))
    )
  
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    replicatedEntityTestkit.clear()
  }
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  import BankAccountBehavior._
  
  "A BankAccountBehavior" should {
    
    "increase a balance when it receives Deposit" in {
      val result = replicatedEntityTestkit.runCommand[DepositSuccess](Deposit(amount = 1000, _))
      result.reply should be(DepositSuccess(amount = 1000))
      result.event should be(Deposited(amount = 1000))
      result.stateOfType[Account].balance should be(1000)
    }
  }
}
```
