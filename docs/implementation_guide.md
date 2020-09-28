# Implementation Guide

akka-entity-replication supports *Event Sourcing* and *Command Query Responsibility Segregation* (CQRS). There are differences between command side implementation and query side implementation, which we will discuss in the respective chapters.

## Command Side

akka-entity-replication supports event sourcing and entity replication with the `ReplicatonActor` trait. An actor that extends this trait can use `replicate` method to replicate and handle events.

`ReplicationActor` behavior is implemented by overriding `receiveCommand` method and `receiveReplica` method.

### Example

```scala
import akka.actor.Props
import lerna.akka.entityreplication._

object BankAccountActor {

    def props: Props = Props(new BankAcountActor())

    sealed trait Command {
        def accountNo: String
    }
    final case class Deposit(accountNo: String, amount: Interger)  extends Commnad
    final case class Withdraw(accountNo: String, amount: Interger) extends Commnad
    final case class GetBalance(accountNo: String)                 extends Command

    final case object ShortBalance
    final case class AccountBalance(balance: Integer)

    sealed trait DomainEvent
    final case class Deposited(amount: Interger)  extends DomainEvent
    final case class Withdrawed(amount: Interger) extends DomainEvent

    final case class Account(balance: Integer) {
        def deposit(amount: Integer)  = copy(balance = balance + amount)
        def withdraw(amount: Integer) = copy(balance = balance - amount)
    }
}

import BankAccountActor._

class BankAccountActor extends ReplicatonActor[Account] {

    private[this] var account: Account = Account(balance = 0)
    
    // to provide snapshot
    override def currentState: Account = account

    override def receiveCommand: Receive = {
        case Deposit(_, amount) =>
            replicate(Deposited(amount)) { event =>
                updateState(event)
            }
        case Withdrawe(_, amount) if amount > account.balance =>
            ensureConsistency {
                sender() ! ShortBalance
            }
        case Withdrawe(_, amount) =>         
            replicate(Withdrawed(amount)) { event =>
                updateState(event)
            }
        case GetBalance(_) =>
            ensureConsistency {
                sender() ! AccountBalance(account.balance)
            }
    }

    override def receiveReplica: Receive = {
        case event: DomainEvent =>
            updateState(event)
        case SnapshotOffer(_, snapshot: Account) =>
            account = snapshot
    }
    
    def updateState(event: DomainEvent): Unit = event match {
        case Deposited(amount) =>
            account = account.deposit(amount)
        case Withdrawed(amount) =>
            account = account.withdraw(amount)
    }
}

```

This example has two data types `Command` and `DomainEvent` (as sealed trait) to represent commands and events of the entity. State of the entity is `Account`. The state contains balance of the account. 

The replication actor handles commands using `receiveCommand` method. A command  is handled by creating an event. Events which are passed to `replica` method are sent to all other replicas of the entity and updates replicas state. The second argument to the `replica` method is a callback that will be called after the entity's replica state update is complete. You can receive the same events as the first argument in the callback. In a callback, an entity state update is performed based on an event. In the example, the `Deposit` and `Withdraw` commands perform these operations.

Operations that inform the client of the state without updating the entity's state are checked for consistency by calling `ensureConsistency` instead of `replicate`. The example calls this method when querying the account balance with the `GetBalance` command.

The `receiveReplica` method of the replication actor implements the process of receiving an event. The event is received when a replica of another entity sends an event in the `replica` method, or when an event created immediately after the creation of the replication actor is replayed to restore the entity's state.


The `currentState` method of the replication actor doesn't directly affect the behavior of the entity; the replication actor implementer has to provide the state of the entity via this method. This state is used to automatically create a snapshot of the entity. Snapshots can reduce memory usage and reduce the time to restore an entity.

To invoke the replication actor in your application, use the `ClusterReplication` extension. When the `ReplicationRegion` is invoked using the extension, replication actors can be used. Here is an example.

The `ClusterReplication` extension is used to start the replication actor in your application. The replication actor is enabled by start the `ReplicationRegion` with the extension. Here's an example.

```scala
import lerna.akka.entityreplication._
import BankAccountActor._

val extractEntityId: ReplicationRegion.ExtractEntityId = {
    case command: Command => (command.accountNo, payload)
}

val nrOfShard = 256

val extractShardId: ReplicationRegion.ExtractShardId = {
    case command: Command => (Math.abs(command.accountNo.hashCode) % nrOfShard).toString
}

val bankAccountReplicationRegion: ActorRef = ClusterReplication(system).start(
    typeName = "BankAccount",
    entityProps = BankAcountActor.props,
    settings = ClusterReplicationSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId,
)
```
To start `RepliationRegion`, specify the `typeName` to identify the type of the Region, and specify the entity to be started under the Region (replication actor) in `entityProps`.  Then, create a `ClusterReplicationSettings` and set it to `settings`.
In addition, specify extractEntityId and `extractShardId` respectively so that the entity identifier (entity ID) and the identifier to separate the entity into groups (shard ID) can be retrieved from the message.
For the `ReplicationRegion` to work properly, the `extractEntityId` and `extractShardId` must be implemented consistently on all nodes.


When sending commands to an entity, they are sent via the `ReplicationRegion`.

```scala
bankAccountReplicationRegion ! BankAcountActor.Deposit(accountNo = "0001", 1000)
```

### Configuration

On the command side, there are the following settings.

```
lerna.akka.entityreplication {
    util {
        // Settings for a component to guarantee completion of a process to prevent timeouts during rolling update.
        at-least-once-complete {
            // Interval to retry until the process (Future) is completed
            retry-interval = 1 second
        }
    }

    // Time to interrupt replication for events that are taking too long
    replication-timeout = 3000 ms

    raft {
        // The time it takes to start electing a new leader after the heartbeat is no longer received from the leader.
        election-timeout = 750 ms
        
        // The interval between leaders sending heartbeats to their followers
        heartbeat-interval = 100 ms
        
        // A role to identify the nodes to place replicas on
        // The number of roles is the number of replicas. It is recommended to set up at least three roles.
        multi-raft-roles = ["replica-default"]

        // Time to keep a cache of snapshots in memory
        snapshot-cache-time-to-live = 10s
        
        // Threshold for saving snapshots and compaction of the log
        snapshot-log-size-threshold = 10000
        
        // Time interval to check the size of the log and check if a snapshotting is needed to be taken
        snapshot-interval = 10s
    }
}
```

## Read Side

akka-entity-replication supports the Command Query Responsibility Segregation (CQRS) implementation, which provides a way to build a data model (read model) for queries based on events which are generated by command side.

### Example

To update a read model, implement the `EventHandler`.

```scala
import lerna.akka.entityreplication._

class ReadModelUpdater(db: DB) extends EventHandler {

    override def fetchOffsetUuid(): Future[Option[UUID]] = db.selectOffsetUuid()

    override def handleFlow(): Flow[EventEnvelope, Done, NotUsed] =
        Flow[EventEnvelope].mapAsync(parallelism = 1) { eventEnvelope =>
          eventEnvelope.event match {
              case Deposited(amount) =>
                  db.updateTransactionAmount(amount)
                  db.updateOffsetUuid(eventEnvelope.offset)
              case Withdrawed(amount) =>
                  db.updateTransactionAmount(amount)
                  db.updateOffsetUuid(eventEnvelope.offset)
          }
    }

}
```

The `EventHandler` implements the methods `fetchOffsetUuid` and `handleFlow`. The `handleFlow` method creates a `Flow` of akka streams that receives events and outputs `Done`. This `Flow` implements the event-based update of the read model, and outputs a `Done` when it succeeds. 

Along with updating read model, save the Offset UUID to the datastore to restart the processing from the offset of last applied event on next start up.

The `fetchOffsetUuid` method implements fetching the Offset UUID from the datastore, which is saved in the `handleFlow`.


```
ClusterReplication(system).start(
    typeName = "BankAccount",
    entityProps = BankAcountActor.props,
    settings = ClusterReplicationSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId,
    maybeEventHandler = Option(new ReadModelUpdater(system)),
)
```

To enable EventHandler, set the `maybeEventHandler` at the start of the `ClusterReplication` extension.


### Configuration

On the read side, there are the following settings.

```
lerna.akka.entityreplication.raft.eventhandler {
    event-handler-singleton {
      // (deprecated) Buffer for stop on exit, to avoid deadletter (No need to change).
      stop-self-delay = 5 seconds
    }

    // Settings for saving committed events from each RaftActor
    commit-log-store {
      // Retry setting to prevent events from being lost if commit-log-store(sharding) stops temporarily
      retry {
        attempts = 15
        delay = 3 seconds
      }
    }

    // cassandra-journal & cassandra-query-journal to save committed events (No need to change)
    // It is currently hard-coded in the source code
    cassandra-journal = ${cassandra-journal}
    cassandra-journal = {
      keyspace = "raft_commited_event"

      // Tagging to allow some RaftActor(Shard) to handle individually committed events together(No need to change)
      event-adapters {
        tagging = "lerna.akka.entityreplication.raft.eventhandler.TaggingEventAdapter"
      }
      event-adapter-bindings {
        "lerna.akka.entityreplication.raft.eventhandler.CommittedEvent" = tagging
      }
    }

    cassandra-query-journal = ${cassandra-query-journal}
    cassandra-query-journal = {
      write-plugin = "lerna.akka.entityreplication.raft.eventhandler.cassandra-journal"
    }
}
```
