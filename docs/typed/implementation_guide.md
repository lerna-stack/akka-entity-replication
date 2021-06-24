# Implementation Guide

akka-entity-replication supports *Event Sourcing* and *Command Query Responsibility Segregation* (CQRS).
There are differences between command side implementation and query side implementation, which we will discuss in the respective chapters.

## Command Side

On the command side, an **entity** validates a **command** and issue a **domain event** based on its own state.
The issued domain events update the **state** of the entity.
Domain events are persisted with event sourcing style, and they can restore entity instance even if the entity is removed.
An entity will be replicated across multiple nodes, and even if a node crashes, then other replica of the entity will respond immediately to provide high availability.
Entity replication is achieved with Raft consensus protocol which synchronized domain events sequence between nodes.

We can achieve these features the following APIs mainly.

- `ReplicatedEntityBehavior`
- `ClusterReplication`

### Example

Following example is minimum implementation for a `ReplicatedEntityBehavior`:

```scala
import lerna.akka.entityreplication.typed._

object MyReplicatedEntity {
  
  val TypeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("MyEntity")
  
  sealed trait Command
  sealed trait DomainEvent
  final case class State()
    
  // expose this method for testing purpose
  def apply(entityContext: ReplicatedEntityContext[Command]): Behavior[Command] =
    ReplicatedEntityBehavior[Command, DomainEvent, State](
      entityContext,
      emptyState = State(),
      commandHandler = (state, cmd) => ???, // TODO: process the command and return an Effect
      eventHandler = (state, evt) => ???,   // TODO: process the event and return the next state
    )
}
```

`ReplicatedEntityBehavior` defines entity behavior.
`ReplicatedEntityBehavior` has APIs similar [EventSourcedBehavior](https://doc.akka.io/docs/akka/2.6/typed/persistence.html) of Akka.

- `entityContext` provides information entity required such as entity identifier
- `emptyState` is the `State` when the entity is created first (e.g. a Counter would start with 0 as state)
- `commandHandler` defines how to handle command by producing `Effect` (e.g. replicate events, reply a message for the command)
- `eventHandler` returns new state which is created from the current state and a replicated event (e.g. Counter grows by Increment event)

To make entities available, use the `ClusterReplication` extension.

```scala
import akka.actor.typed.ActorSystem
import lerna.akka.entityreplication.typed._

val system: ActorSystem[_] = ???

val clusterReplication = ClusterReplication(system)

// send command to a entity via replication region
val region: ActorRef[ReplicationEnvelope[Command]] = 
  clusterReplication.init(ReplicatedEntity(MyReplicatedEntity.TypeKey)(entityContext => MyReplicatedEntity(entityContext)))

region ! ReplicationEnvelope("entity-1", DoSomething())

// send command to a entity via ReplicatedEntityRef
val entityRef: ReplicatedEntityRef[Command] = clusterReplication.entityRefFor(MyReplicatedEntity.TypeKey, "entity-1")

entityRef ! DoSomething()
```

`ClusterReplication.init(...)` enables the entity.
There are two ways to send commands to an entity:
either by sending a `ReplicationEnvelope` to the **replication region**, or by sending the command directly to the `ReplicatedEntityRef`.
`ClusterReplication.init` provides the `ActorRef` for the replication region.
You can get the `ReplicatedEntityRef` from `ClusterReplication.entityRefFor(...)`.
Each entity has a key to identify its type, which is defined by `ReplicatedEntityTypeKey`.
The methods described so far, such as `init` and `entityRefFor`, use this key to identify the type of the entity.

### Entity Implementation

A following more detailed example illustrate how to implement entity specifically.

```scala
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import lerna.akka.entityreplication.typed._

import scala.concurrent.duration._
import scala.collection.immutable.ListMap

object BankAccountBehavior {

  val TypeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("BankAccount")

  sealed trait Command
  final case class Deposit(transactionId: Long, amount: BigDecimal, replyTo: ActorRef[DepositSucceeded]) extends Command
  final case class Withdraw(transactionId: Long, amount: BigDecimal, replyTo: ActorRef[WithdrawReply])   extends Command
  final case class GetBalance(replyTo: ActorRef[AccountBalance])                                         extends Command
  final case class ReceiveTimeout()                                                                      extends Command
  final case class Stop()                                                                                extends Command
  // DepositReply
  final case class DepositSucceeded(balance: BigDecimal)
  sealed trait WithdrawReply
  final case class ShortBalance()                         extends WithdrawReply
  final case class WithdrawSucceeded(balance: BigDecimal) extends WithdrawReply
  // GetBalanceReply
  final case class AccountBalance(balance: BigDecimal)

  sealed trait DomainEvent
  final case class Deposited(transactionId: Long, amount: BigDecimal) extends DomainEvent
  final case class Withdrew(transactionId: Long, amount: BigDecimal)  extends DomainEvent
  final case class BalanceShorted(transactionId: Long)                extends DomainEvent

  type Effect = lerna.akka.entityreplication.typed.Effect[DomainEvent, Account]

  final case class Account(balance: BigDecimal, resentTransactions: ListMap[Long, DomainEvent]) {

    def deposit(amount: BigDecimal): Account =
      copy(balance = balance + amount)

    def withdraw(amount: BigDecimal): Account =
      copy(balance = balance - amount)

    private[this] val maxResentTransactionSize = 30

    def recordEvent(transactionId: Long, event: DomainEvent): Account =
      copy(resentTransactions = (resentTransactions + (transactionId -> event)).takeRight(maxResentTransactionSize))

    def applyCommand(command: Command): Effect =
      command match {
        case Deposit(transactionId, amount, replyTo) =>
          if (resentTransactions.contains(transactionId)) {
            Effect.reply(replyTo)(DepositSucceeded(balance))
          } else {
            Effect
              .replicate(Deposited(transactionId, amount))
              .thenReply(replyTo)(state => DepositSucceeded(state.balance))
          }
        case Withdraw(transactionId, amount, replyTo) =>
          resentTransactions.get(transactionId) match {
            // Receive a known transaction: replies message based on stored event in resetTransactions
            case Some(_: Withdrew) =>
              Effect.reply(replyTo)(WithdrawSucceeded(balance))
            case Some(_: BalanceShorted) =>
              Effect.reply(replyTo)(ShortBalance())
            case Some(_: Deposited) =>
              Effect.unhandled.thenNoReply()
            // Receive an unknown transaction
            case None =>
              if (balance < amount) {
                Effect
                  .replicate(BalanceShorted(transactionId))
                  .thenReply(replyTo)(_ => ShortBalance())
              } else {
                Effect
                  .replicate(Withdrew(transactionId, amount))
                  .thenReply(replyTo)(state => WithdrawSucceeded(state.balance))
              }
          }
        case GetBalance(replyTo) =>
          Effect.reply(replyTo)(AccountBalance(balance))
        case ReceiveTimeout() =>
          Effect.passivate().thenNoReply()
        case Stop() =>
          Effect.stopLocally()
      }

    def applyEvent(event: DomainEvent): Account =
      event match {
        case Deposited(transactionId, amount) => deposit(amount).recordEvent(transactionId, event)
        case Withdrew(transactionId, amount)  => withdraw(amount).recordEvent(transactionId, event)
        case BalanceShorted(transactionId)    => recordEvent(transactionId, event)
      }
  }

  def apply(entityContext: ReplicatedEntityContext[Command]): Behavior[Command] = {
    Behaviors.setup { context =>
      // This is highly recommended to identify the source of log outputs
      context.setLoggerName(BankAccountBehavior.getClass)
      // ReceiveTimeout will trigger Effect.passivate()
      context.setReceiveTimeout(1.minute, ReceiveTimeout())
      ReplicatedEntityBehavior[Command, DomainEvent, Account](
        entityContext,
        emptyState = Account(BigDecimal(0), ListMap()),
        commandHandler = (state, cmd) => state.applyCommand(cmd),
        eventHandler = (state, evt) => state.applyEvent(evt),
      ).withStopMessage(Stop())
    }
  }
}
```

This example has two data types `Command` and `DomainEvent` (as sealed trait) to represent commands and events of the entity.
State of the entity is `Account`. The state contains balance of the account.

The entity handles commands using `commandHandler`. The handler returns `Effect`.
`reply` effect send a reply message to the given `replyTo`.
`replicate` effect will persist an event and send the event to other entity replica to synchronize these state.
The example entity handles `Deposit` command and replicates `Deposited` event and then replies `DepositSucceeded` message to the `ActorRef` which the command has.
For more details about `Effect`, see "Effects" section below.

The replicated event will be handled by `eventHandler`. 
The handler returns state of the entity which updated by the replicated event.
In this example, the `balance` of `Account` is increased by `Deposited` event. 

Now we can send commands to an entity via `ClusterReplication` as follows.

```scala
import akka.actor.typed.ActorSystem
import lerna.akka.entityreplication.typed._

val system: ActorSystem[_] = ???

val clusterReplication = ClusterReplication(system)

clusterReplication.init(ReplicatedEntity(BankAccountBehavior.TypeKey)(entityContext => BankAccountBehavior(entityContext)))

val accountNo = "0001"
val entityRef: ReplicatedEntityRef[BankAccountBehavior.Command] = 
  clusterReplication.entityRefFor(BankAccountBehavior.TypeKey, accountNo)

val reply: Future[DepositSucceeded] = 
  entityRef ? BankAccountBehavior.Deposit(transactionId = 1L, amount = 1000, _)
```

### Effects

A `commandHandler` returns a `Effect` that defines what the entity do for a command.
Effects are created by `Effect` factory and can be one of:

- `replicate` will persist an event and send the event to other entity replica to synchronize these state
- `none` no events to replicate (e.g. process a read-only command)
- `unhandled` the command is not handled because it is not supported in current state
- `passivate` will passivate all replicas in multiple nodes of the entity
- `stopLocally` stop this actor locally (not effects entity replica in other nodes)
- `stash` the current command is stashed
- `unstashAll` process the commands that were stashed with `stash`
- `reply` send a reply message to the given `ActorRef` without replicating new events
- `noReply` nothing to do

In addition to the primary `Effect` can also chain other effects that performs after successful `replicate`.
For example `thenRun` effect registers callbacks that performs after successful `replicate`.

The available effects are as follows:

- `thenRun` run arbitrary actions (e.g. output log)
- `thenStopLocally` stop this actor locally (not effects entity replica in other nodes)
- `thenPassivate` will passivate all replicas in multiple nodes of the entity
- `thenUnstashAll` process the commands that were stashed with `stash`
- `thenNoReply` indicates that the entity will not reply any message for the command ("tell" style interaction)
- `thenReply` send a reply message which is created based on latest state to the given `ActorRef`

Consistency is ensured when it processes operations that can effect outside the entity (such as `thenRun`, `thenReply`).
The entity will output results base on the consistent up-to-date state even if under the network partitioning. 
The commands will be fail on one side of the partitioned network to keep consistency.

### Passivation

You can stop entities that are not used to reduce memory consumption.
This is done by the application specific implementation of the entity.
For example, to stop the entity when there is no command for a certain period of time,
use `ActorContext.setReceiveTimeout` and handles the command that is emitted by the timer with `Effect.passivate`.

By default, the entity implicitly stop with `PoisonPill`.
If you want to hold off on stopping the entity depending on its status (e.g. waiting for a response from external system),
you can define application specific message to stop entities with `ReplicatedEntityBehavior.withStopMessage`.

### Logger name

By default, you can't identify an entity class from log output.
Also, logging libraries such as logback allow you to adjust the log output individually by the logger name.
It is highly recommended to set custom logger name with `ActorContext.setLoggerName`.

```scala
def apply(entityContext: ReplicatedEntityContext[Command]): Behavior[Command] = {
  Behaviors.setup { context =>
    // This is highly recommended to identify the source of log outputs
    context.setLoggerName(MyReplicatedEntity.getClass)
    ReplicatedEntityBehavior[Command, DomainEvent, Account](
      ...
    )
  }
}
```

### Reliable command delivery

To reduce errors, it is recommended to perform retry processing so that processing continues even if a single Node fails.
You can use `AtLeastOnceComplete.askTo` to retry until Future is complete, as shown below.

```scala
import akka.actor.typed.ActorSystem
import akka.util.Timeout
import lerna.akka.entityreplication.util.AtLeastOnceComplete

import scala.concurrent.duration._

implicit val timeout: Timeout       = Timeout(3.seconds) // should be greater than or equal to retryInterval
implicit val system: ActorSystem[_] = ???                // pass one that is already created

val accountNo = "0001"
val entityRef: ReplicatedEntityRef[BankAccountBehavior.Command] =
  clusterReplication.entityRefFor(BankAccountBehavior.TypeKey, accountNo)
val transactionId = ??? // generate unique ID

val reply: Future[DepositSucceeded] =
  AtLeastOnceComplete.askTo(
    destination = entityRef,
    message = BankAccountBehavior.Deposit(transactionId, amount = 1000, _),
    retryInterval = 500.milliseconds,
  )
```

Note that `AtLeastOnceComplete` may cause that the entity receives again the command that has already completed.
The `BankAccountBehavior` example implements uses `transactionId` to avoid duplicate commands.

### Change persistence plugins programmatically

```scala
import akka.actor.typed.ActorSystem
import lerna.akka.entityreplication.typed._

val system: ActorSystem[_] = ???
val clusterReplication = ClusterReplication(system)

// specify persistence plugin ids
val settings =
  ClusterReplicationSettings(system)
    .withRaftJournalPluginId("my.special.raft.journal")
    .withRaftSnapshotPluginId("my.special.raft.snapshot-store")
    .withRaftQueryPluginId("my.special.raft.query")
    .withEventSourcedJournalPluginId("my.special.eventsourced.journal")

val entity = 
  ReplicatedEntity(BankAccountBehavior.TypeKey)(entityContext => BankAccountBehavior(entityContext))
    .withSettings(settings)
    
clusterReplication.init(entity)
```

### Configuration

On the command side, there are the following settings.

```hocon
lerna.akka.entityreplication {

    // How long wait before giving up entity recovery.
    // Entity recovery requires a snapshot, and failure fetching it will cause this timeout.
    // If timed out, entity recovery will be retried.
    recovery-entity-timeout = 10s

    raft {
        // The time it takes to start electing a new leader after the heartbeat is no longer received from the leader.
        election-timeout = 750 ms
        
        // The interval between leaders sending heartbeats to their followers
        heartbeat-interval = 100 ms
        
        // A role to identify the nodes to place replicas on
        // The number of roles is the number of replicas. It is recommended to set up at least three roles.
        multi-raft-roles = ["replica-group-1", "replica-group-2", "replica-group-3"]

        // Number of shards per single multi-raft-role used by only typed APIs.
        // This value must be the same for all nodes in the cluster
        // and must not be changed after starting to use.
        // Changing this value will cause data inconsistency.
        number-of-shards = 100
      
        // Maximum number of entries which AppendEntries contains.
        // The too large size will cause message serialization failure.
        max-append-entries-size = 16
  
        // The maximum number of AppendEnteis that will be sent at once at every heartbeat-interval.
        max-append-entries-batch-size = 10
      
        // log compaction settings
        compaction {

          // Time interval to check the size of the log and check if a snapshotting is needed to be taken
          log-size-check-interval = 10s

          // Threshold for saving snapshots and compaction of the log.
          // If this value is too large, your application will use a lot of memory and you may get an OutOfMemoryError.
          // If this value is too small, it compaction may occur frequently and overload the application and the data store.
          log-size-threshold = 50000

          // Preserving log entries from log reduction to avoid log replication failure.
          // If more number of logs than this value cannot be synchronized, the raft member will be unavailable.
          // It is recommended to set this value even less than log-size-threshold. Otherwise compaction will be run at every log-size-check-interval.
          preserve-log-size = 10000

          // Time to keep a cache of snapshots in memory
          snapshot-cache-time-to-live = 10s
        }

        // snapshot synchronization settings
        snapshot-sync {
  
          // Number of snapshots of entities that are copied in parallel
          snapshot-copying-parallelism = 10
  
          // Time to abort operations related to persistence
          persistence-operation-timeout = 10s
        }

        // data persistent settings
        persistence {
          // Absolute path to the journal plugin configuration entry.
          // The journal will be stored events which related to Raft.
          journal.plugin = ""

          // Absolute path to the snapshot store plugin configuration entry.
          // The snapshot store will be stored state which related to Raft.
          snapshot-store.plugin = ""

          // Absolute path to the query plugin configuration entry.
          // Snapshot synchronization reads events that related to Raft.
          query.plugin = ""
        }
    }
}
```

## Read Side

akka-entity-replication supports the Command Query Responsibility Segregation (CQRS) implementation,
which provides a way to build a data model (read model) for queries based on events which are generated by command side.

### Example

First, building read model data requires the following preparations.

- Create `EventAdapter` for tagging events
- Add a setting to the configuration of a journal plugin to enable the `EventAdapter`

The following `BankAccountEventAdapter` example tags the `DomainEvent` of `BankAccountActor` with the tag `"bank-account-transaction"`. 
For more details about `EventAdapter`, see [this Akka official document](https://doc.akka.io/docs/akka/2.6/persistence.html#event-adapters).

```scala
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.journal.{Tagged, WriteEventAdapter}

class BankAccountEventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {

  private[this] val log = Logging(system, getClass)

  override def manifest(event: Any): String = "" // when no manifest needed, return ""

  override def toJournal(event: Any): Any = {
    event match {
      case domainEvent: BankAccountActor.DomainEvent =>
        val tags: Set[String] = Set(
          "bank-account-transaction",
        )
        Tagged(domainEvent, tags)
      case _ =>
        log.warning("unexpected event: {}", event)
        event // identity
    }
  }
}
```

The following configuration example sets `BankAccountEventAdapter` to cassandra journal plugin which used by event writer in akka-entity-replication.

```hocon
akka-entity-replication.eventsourced.persistence.cassandra.journal {
  // Tagging to allow some RaftActor(Shard) to handle individually committed events together(No need to change)
  event-adapters {
    bank-account-tagging = "com.example.BankAccountEventAdapter"
  }
  event-adapter-bindings {
    // bank-account-tagging takes events which mixins BankAccount$DomainEvent
    "com.example.BankAccountBehavior$DomainEvent" = bank-account-tagging
  }
}
```

To update a read model, implement Handler with [Akka Projection](https://doc.akka.io/docs/akka-projection/1.1.0/overview.html).
It can read tagged events and update the read model.
Using Akka Projection requires adding dependencies to your project first.
For more details, see [Akka Projection official document](https://doc.akka.io/docs/akka-projection/1.1.0/overview.html).

In the case of [SlickHandler](https://doc.akka.io/docs/akka-projection/1.1.0/slick.html), it will be as follows.

```scala
class EventHandler(actions: StatisticsActions) extends SlickHandler[EventEnvelope[Event]] {
  override def process(envelope: EventEnvelope[Event]): DBIO[Done] = {
    envelope.event match {
      case Deposited(amount) =>
        actions.insertDepositRecord(amount)
      case Withdrawed(amount) =>
        actions.insertWithdrawalRecord(amount)
    }
  }
}
```

The definition for starting the defined Handler is as follows.

```scala
import akka.projection.eventsourced.scaladsl.EventSourcedProvider

object EventHandler {
  def start(
      actions: StatisticsActions,
      databaseConfig: DatabaseConfig[JdbcProfile],
  )(implicit
      system: ActorSystem[_],
  ): ActorRef[ProjectionBehavior.Command] = {
    
    val sourceProvider =
      EventSourcedProvider.eventsByTag[BankAccountActor.DomainEvent](
        system,
        // Note: You have to set a configuration key of *Query* Plugin, NOT Journal Plugin
        readJournalPluginId = "akka-entity-replication.eventsourced.persistence.cassandra.query",
        tag = "bank-account-transaction"
      )
    
    def generateProjection(): ExactlyOnceProjection[Offset, EventEnvelope[Event]] =
      SlickProjection.exactlyOnce(
        projectionId = ProjectionId(name = "BankAccount", key = "aggregate"),
        sourceProvider = sourceProvider,
        databaseConfig = databaseConfig,
        handler = () => new EventHandler(actions),
      )

    val projection = generateProjection()
    ClusterSingleton(system).init(SingletonActor(ProjectionBehavior(projection), projection.projectionId.id))
  }
}
```

`ProjectionId` is used to identify an offset in data store.
You can set an arbitrary value however you cannot change the value easily after run the projection.

### Tips
- If you want to use Handler and Projection other than Slick, please refer to [the official Akka documentation](https://doc.akka.io/docs/akka-projection/1.1.0/overview.html).
- Akka projection requires typed ActorSystem.
    - Conversion from classic ActorSystem to typed ActorSystem is possible with `import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps` and `system.toTyped` (see also: [Coexistence - Akka Documentation](https://doc.akka.io/docs/akka/2.6/typed/coexisting.html#classic-to-typed)).

### Configuration

On the read side, there are the following settings.

```hocon
lerna.akka.entityreplication.raft.eventsourced {
    // Settings for saving committed events from each RaftActor
    commit-log-store {
      // Retry setting to prevent events from being lost if commit-log-store(sharding) stops temporarily
      retry {
        attempts = 15
        delay = 3 seconds
      }
    }

    persistence {
      // Absolute path to the journal plugin configuration entry.
      // The journal stores Raft-committed events.
      journal.plugin = ""
    }
}
```

## Persistence plugin configuration

By default, the persistence plugin configurations are empty (`""`) in [reference.conf](/src/main/resources/reference.conf):

```hocon
// Command side persistence plugin settings
lerna.akka.entityreplication.raft.persistence {
    journal.plugin        = ""
    snapshot-store.plugin = ""
}

// Query side persistence plugin settings
lerna.akka.entityreplication.raft.eventsourced.persistence {
    journal.plugin  = ""
}
```

It requires explicit user configuration by overriding them in the application.conf.

For an example configuration to use Cassandra as a data store with [akka-persistence-cassandra](https://doc.akka.io/docs/akka-persistence-cassandra/current/) see [akka-entity-replication-with-cassandra.conf](/src/test/resources/akka-entity-replication-with-cassandra.conf).

Persistence plugins to set can be selected.
For more details see [Akka Persistence Plugins official document](https://doc.akka.io/docs/akka/current/persistence-plugins.html)

Make sure the configuration has as low as possible risk of data loss to ensure consistency.
(e.g. In Cassandra, set replication-factor larger than 2, and set consistency level higher than LOCAL_QUORUM)

The data durability required by the command side, and the query side is different.

The command side is more durable because the data is replicated by the Raft protocol. However,
it is recommended to maintain durability using the data store because this extension does not currently have sufficient recovery capabilities in case of data loss.
This recommendation may be changed in a future release.

The query side data is not replicated like the command side data, so the data store should ensure durability.
Otherwise, the query side may fail to update data.

## Serializer Configuration

Commands, events, and states of an entity should be serializable.
You have to configure a serializer that serializes these instances.
akka-entity-replication uses the serialization mechanism in *Akka*.
Therefore, you can configure a serializer of commands, events, and states in `application.conf` like below.

```hocon
akka {
  actor {
    serializers {
      jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
    }
    serialization-bindings {
      "com.example.BankAccountBehavior$Command" = jackson-json
      "com.example.BankAccountBehavior$DomainEvent" = jackson-json
      "com.example.BankAccountBehavior$Account" = jackson-json
    }
  }
}
```

Although the above example configuration uses [Jackson](https://github.com/FasterXML/jackson) as the serializer, you can use your favorite serializer.
For more details, See [Serialization](https://doc.akka.io/docs/akka/current/serialization.html).
