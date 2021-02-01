# Implementation Guide

akka-entity-replication supports *Event Sourcing* and *Command Query Responsibility Segregation* (CQRS). There are differences between command side implementation and query side implementation, which we will discuss in the respective chapters.

## Command Side

akka-entity-replication supports event sourcing and entity replication with the `ReplicatonActor` trait. An actor that extends this trait can use `replicate` method to replicate and handle events.

`ReplicationActor` behavior is implemented by overriding `receiveCommand` method and `receiveReplica` method.

### Example

```scala
import akka.actor.Props
import lerna.akka.entityreplication._
import lerna.akka.entityreplication.raft.protocol.SnapshotOffer

object BankAccountActor {

    def props: Props = Props(new BankAccountActor())

    sealed trait Command {
        def accountNo: String
    }
    final case class Deposit(accountNo: String, amount: Int)  extends Command
    final case class Withdraw(accountNo: String, amount: Int) extends Command
    final case class GetBalance(accountNo: String)            extends Command

    final case object ShortBalance
    final case class AccountBalance(balance: Int)

    sealed trait DomainEvent
    final case class Deposited(amount: Int)   extends DomainEvent
    final case class Withdrawed(amount: Int)  extends DomainEvent

    final case class Account(balance: Int) {
        def deposit(amount: Int)  = copy(balance = balance + amount)
        def withdraw(amount: Int) = copy(balance = balance - amount)
    }
}

import BankAccountActor._

class BankAccountActor extends ReplicationActor[Account] {

    private[this] var account: Account = Account(balance = 0)
    
    // to provide snapshot
    override def currentState: Account = account

    override def receiveCommand: Receive = {
        case Deposit(_, amount) =>
            replicate(Deposited(amount)) { event =>
                updateState(event)
            }
        case Withdraw(_, amount) if amount > account.balance =>
            ensureConsistency {
                sender() ! ShortBalance
            }
        case Withdraw(_, amount) =>         
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
        case SnapshotOffer(snapshot: Account) =>
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
    entityProps = BankAccountActor.props,
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
bankAccountReplicationRegion ! BankAccountActor.Deposit(accountNo = "0001", 1000)
```

To reduce errors, it is recommended to perform retry processing so that processing continues even if a single Node fails.
You can use `AtLeastOnceComplete.askTo` to retry until Future is complete, as shown below.

```scala
import akka.actor.ActorSystem
import akka.util.Timeout
import lerna.akka.entityreplication.util.AtLeastOnceComplete

import scala.concurrent.duration._

implicit val timeout: Timeout    = Timeout(3.seconds) // should be greater than or equal to retryInterval
implicit val system: ActorSystem = ???                // pass one that is already created

AtLeastOnceComplete.askTo(
  destination = bankAccountReplicationRegion,
  message = BankAccountActor.Deposit(accountNo = "0001", 1000),
  retryInterval = 500.milliseconds,
)
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

        // data persistent settings
        persistence {
          // Absolute path to the journal plugin configuration entry.
          // The journal will be stored events which related to Raft.
          journal.plugin = "lerna.akka.entityreplication.raft.persistence.cassandra.journal"
    
          // Absolute path to the snapshot store plugin configuration entry.
          // The snapshot store will be stored state which related to Raft.
          snapshot-store.plugin = "lerna.akka.entityreplication.raft.persistence.cassandra.snapshot"
        }

        // The settings for Cassandra persistence plugin
        persistence.cassandra = ${akka.persistence.cassandra}
        persistence.cassandra {
    
          // Profile to use.
          // See https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/ for overriding any settings
          read-profile = "akka-entity-replication-profile"
          write-profile = "akka-entity-replication-profile"

          journal {
    
            // replication strategy to use.
            replication-strategy = "NetworkTopologyStrategy"
    
            // Replication factor list for data centers, e.g. ["dc0:3", "dc1:3"]. This setting is only used when replication-strategy is NetworkTopologyStrategy.
            // Replication factors should be 3 or more to maintain data consisstency.
            data-center-replication-factors = ["dc0:3"]
        
            // Name of the keyspace to be used by the journal
            keyspace = "entity_replication"
          }
    
          snapshot {
    
            // Profile to use.
            // See https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/ for overriding any settings
            read-profile = "akka-entity-replication-snapshot-profile"
            write-profile = "akka-entity-replication-snapshot-profile"

            // replication strategy to use.
            replication-strategy = "NetworkTopologyStrategy"
    
            // Replication factor list for data centers, e.g. ["dc0:3", "dc1:3"]. This setting is only used when replication-strategy is NetworkTopologyStrategy.
            // Replication factors should be 3 or more to maintain data consisstency.
            data-center-replication-factors = ["dc0:3"]
        
            // Name of the keyspace to be used by the snapshot store
            keyspace = "entity_replication_snapshot"
          }
        }
    }
}
```

## Read Side

akka-entity-replication supports the Command Query Responsibility Segregation (CQRS) implementation, which provides a way to build a data model (read model) for queries based on events which are generated by command side.

### Example

To update a read model, implement Handler with [Akka Projection](https://doc.akka.io/docs/akka-projection/1.0.0/overview.html).

In the case of [SlickHandler](https://doc.akka.io/docs/akka-projection/1.0.0/slick.html), it will be as follows.

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
import lerna.akka.entityreplication.raft.eventhandler.EntityReplicationEventSource

object EventHandler {
  def start(
      actions: StatisticsActions,
      databaseConfig: DatabaseConfig[JdbcProfile],
  )(implicit
      system: ActorSystem[_],
  ): ActorRef[ProjectionBehavior.Command] = {
    def generateProjection(): ExactlyOnceProjection[Offset, EventEnvelope[Event]] =
      SlickProjection.exactlyOnce(
        projectionId = ProjectionId(name = "BankAccount", key = "aggregate"),
        sourceProvider = EntityReplicationEventSource.sourceProvider,
        databaseConfig = databaseConfig,
        handler = () => new EventHandler(actions),
      )

    val projection = generateProjection()
    ClusterSingleton(system).init(SingletonActor(ProjectionBehavior(projection), projection.projectionId.id))
  }
}
```

`ProjectionId` is used to identify an offset in data store. You can set an arbitrary value however you cannot change the value easily after run the projection.
`EntityReplicationEventSource.sourceProvider` should be set to `sourceProvider`. `EntityReplicationEventSource.sourceProvider` provides events which were produced in command side of akka-entity-replication.

### Tips
- If you want to use Handler and Projection other than Slick, please refer to [the official Akka documentation](https://doc.akka.io/docs/akka-projection/1.0.0/overview.html).
- Akka projection requires typed ActorSystem.
    - Conversion from classic ActorSystem to typed ActorSystem is possible with `import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps` and `system.toTyped` (see also: [Coexistence - Akka Documentation](https://doc.akka.io/docs/akka/2.6/typed/coexisting.html#classic-to-typed)).

### Configuration

On the read side, there are the following settings.

```hocon
lerna.akka.entityreplication.raft.eventhandler {
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
      journal.plugin = "lerna.akka.entityreplication.raft.eventhandler.persistence.cassandra.journal"

      // Absolute path to the query plugin configuration entry.
      // The query is used by Raft EventHandler.
      query.plugin = "lerna.akka.entityreplication.raft.eventhandler.persistence.cassandra.query"
    }

    // cassandra-journal & cassandra-query-journal to save committed events
    persistence.cassandra = ${akka.persistence.cassandra}
    persistence.cassandra = {

      // Profile to use.
      // See https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/ for overriding any settings
      read-profile = "akka-entity-replication-profile"
      write-profile = "akka-entity-replication-profile"

      journal {

        // replication strategy to use.
        replication-strategy = "NetworkTopologyStrategy"

        // Replication factor list for data centers, e.g. ["dc0:3", "dc1:3"]. This setting is only used when replication-strategy is NetworkTopologyStrategy.
        // Replication factors should be 3 or more to maintain data consisstency.
        data-center-replication-factors = ["dc0:3"]

        // Name of the keyspace to be used by the journal
        keyspace = "raft_commited_event"

        // Tagging to allow some RaftActor(Shard) to handle individually committed events together(No need to change)
        event-adapters {
          tagging = "lerna.akka.entityreplication.raft.eventhandler.TaggingEventAdapter"
        }
        event-adapter-bindings {
          "java.lang.Object" = tagging
        }
      }
    }
}
```

## Cassandra driver configuration

akka-entity-replication has default profile settings for DataStax Java Driver.

The default settings are bellow.

```hocon
// You can find reference configuration at
// https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/
// see also: https://doc.akka.io/docs/akka-persistence-cassandra/1.0.3/configuration.html#cassandra-driver-configuration
datastax-java-driver {
  
  // The contact points to use for the initial connection to the cluster.
  // basic.contact-points = ["127.0.0.1:9042"]

  // To limit the Cassandra hosts this plugin connects with to a specific datacenter.
  // basic.load-balancing-policy.local-datacenter = "dc0"
  
  profiles {

    // It is recommended to set this value.
    // For more details see https://doc.akka.io/docs/akka-persistence-cassandra/1.0.3/configuration.html#cassandra-driver-configuration
    // advanced.reconnect-on-init = true

    akka-entity-replication-profile {
      basic.request {
        // Important: akka-entity-replication recommends quorum based consistency level to remain data consistency
        consistency = LOCAL_QUORUM
        // the journal does not use any counters or collections
        default-idempotence = true
      }
    }

    akka-entity-replication-snapshot-profile {
      basic.request {
        // Important: akka-entity-replication recommends quorum based consistency level to remain data consistency
        consistency = LOCAL_QUORUM
        // the snapshot store does not use any counters or collections
        default-idempotence = true
      }
    }
  }
}
```

For more details see [Akka Persistence Cassandra official document](https://doc.akka.io/docs/akka-persistence-cassandra/1.0.3/configuration.html#cassandra-driver-configuration).
