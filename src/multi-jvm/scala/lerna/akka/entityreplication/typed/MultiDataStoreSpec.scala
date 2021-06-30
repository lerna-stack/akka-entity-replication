package lerna.akka.entityreplication.typed

import akka.actor.ExtendedActorSystem
import akka.pattern.ask
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.Behaviors
import akka.event.Logging
import akka.persistence.inmemory.extension._
import akka.persistence.inmemory.snapshotEntry
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.util.ByteString
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.typed.ReplicatedEntityMultiNodeSpecConfig.{ memberIndexes, node1, node2, node3 }
import lerna.akka.entityreplication.util.{ ActorIds, AtLeastOnceComplete }

import java.net.URLEncoder
import java.time.Instant
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object MultiDataStoreSpecConfig extends MultiNodeConfig {
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
      .withFallback(ConfigFactory.parseString(s"""
          lerna.akka.entityreplication.raft.compaction {
            log-size-threshold = 2
            preserve-log-size = 1
            log-size-check-interval = 0.1s
          }
          lerna.akka.entityreplication.raft.persistence {
            journal.plugin = akka.persistence.journal.inmem
            snapshot-store.plugin = akka.persistence.snapshot-store.local
            query.plugin = inmemory-read-journal
          }
          lerna.akka.entityreplication.raft.eventsourced.persistence {
            journal.plugin = akka.persistence.journal.inmem
          }
          // save snapshot to unique directory every time
          akka.persistence.snapshot-store.local.dir = "target/snapshots/${Instant.now().getEpochSecond}"
          
          inmemory-journal {
            event-adapters {
              ping-pong-tagging = "lerna.akka.entityreplication.typed.MultiDataStoreSpec$$PingPongEventAdapter"
            }
            event-adapter-bindings {
              "lerna.akka.entityreplication.typed.MultiDataStoreSpec$$PingPongEntity$$Event" = ping-pong-tagging
            }
          }
          
        """))
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node1)}"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2)}"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node3)}"]
  """))
}

class MultiDataStoreSpecMultiJvmNode1 extends MultiDataStoreSpec
class MultiDataStoreSpecMultiJvmNode2 extends MultiDataStoreSpec
class MultiDataStoreSpecMultiJvmNode3 extends MultiDataStoreSpec

class MultiDataStoreSpec extends MultiNodeSpec(MultiDataStoreSpecConfig) with STMultiNodeSpec {

  import MultiDataStoreSpec._

  private[this] implicit val typedSystem: ActorSystem[_] = system.toTyped

  private[this] val clusterReplication = ClusterReplication(typedSystem)

  private[this] val storage = StorageExtension(typedSystem)

  "MultiDataStore" should {

    "wait for all nodes to join the cluster" in {
      joinCluster(node1, node2, node3)
    }

    "initialize ClusterReplication" in {
      clusterReplication.init(PingPongEntity(defaultDataStoreTypeKey, typedSystem))
      clusterReplication.init(PingPongEntity(storageExtensionDataStoreTypeKey, typedSystem))
    }

    /*
     * Rearranging the order of test cases will cause to fail the test:
     * https://github.com/lerna-stack/akka-entity-replication/pull/88#discussion_r661128422
     */

    "not persist data to StorageExtension when use default data store which is defined in configuration" in {

      runOn(node1) {
        val entityRef = clusterReplication.entityRefFor(defaultDataStoreTypeKey, entityId = "test")

        val reply1 = AtLeastOnceComplete.askTo(entityRef, PingPongEntity.Ping(_), retryInterval = 100.millis)
        reply1.await shouldBe a[PingPongEntity.Pong]
        val reply2 = AtLeastOnceComplete.askTo(entityRef, PingPongEntity.Ping(_), retryInterval = 100.millis)
        reply2.await shouldBe a[PingPongEntity.Pong]

        fetchAllJournalEntriesFromSecondaryStorage() should be(empty)
        fetchSnapshots(entityRef.entityId) should be(empty)
      }
    }

    "persist data to StorageExtension when set persistent plugins to ClusterReplicationSettings" in {

      runOn(node1) {
        val entityRef = clusterReplication.entityRefFor(storageExtensionDataStoreTypeKey, entityId = "test")

        val reply1 = AtLeastOnceComplete.askTo(entityRef, PingPongEntity.Ping(_), retryInterval = 100.millis)
        reply1.await shouldBe a[PingPongEntity.Pong]
        val reply2 = AtLeastOnceComplete.askTo(entityRef, PingPongEntity.Ping(_), retryInterval = 100.millis)
        reply2.await shouldBe a[PingPongEntity.Pong]

        fetchAllJournalEntriesFromSecondaryStorage() should not be empty
        fetchSnapshots(entityRef.entityId) should not be empty
      }
    }

  }

  private[this] def fetchAllJournalEntriesFromSecondaryStorage(): Set[String] =
    (storage.journalStorage ? InMemoryJournalStorage.AllPersistenceIds).mapTo[Set[String]].await

  private[this] def fetchSnapshots(entityId: String): Seq[snapshotEntry] = {
    import typedSystem.executionContext
    Future
      .sequence {
        // retrieve snapshots for all member indexes
        memberIndexes.values.map { memberIndex =>
          (storage.snapshotStorage ? InMemorySnapshotStorage.SnapshotForMaxSequenceNr(
            ActorIds.persistenceId(
              "SnapshotStore",
              URLEncoder.encode(storageExtensionDataStoreTypeKey.name, ByteString.UTF_8),
              entityId,
              memberIndex.role,
            ),
            sequenceNr = Long.MaxValue,
          )).mapTo[Option[snapshotEntry]]
        }
      }.map(_.flatten).await.toSeq
  }
}

object MultiDataStoreSpec {

  /**
    * A typeKey which does not use the StorageExtension:
    * It can not check persisted data
    */
  val defaultDataStoreTypeKey: ReplicatedEntityTypeKey[PingPongEntity.Command] =
    PingPongEntity.typeKey("default-data-store")

  /**
    * A typeKey which uses the StorageExtension:
    * It can check persisted data by StorageExtension API
    */
  val storageExtensionDataStoreTypeKey: ReplicatedEntityTypeKey[PingPongEntity.Command] =
    PingPongEntity.typeKey("storage-extension-data-store")

  object PingPongEntity {
    def typeKey(scope: String): ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey(s"PingPong:$scope")

    sealed trait Command                           extends STMultiNodeSerializable
    final case class Ping(replyTo: ActorRef[Pong]) extends Command
    final case class Pong(count: Int)              extends STMultiNodeSerializable

    sealed trait Event         extends STMultiNodeSerializable
    final case class CountUp() extends Event

    final case class State(count: Int) extends STMultiNodeSerializable {

      def applyCommand(message: Command): Effect[Event, State] =
        message match {
          case Ping(replyTo) =>
            Effect
              .replicate(CountUp())
              .thenReply(replyTo)(s => Pong(s.count))
        }

      def applyEvent(event: Event): State =
        event match {
          case CountUp() => copy(count = count + 1)
        }
    }

    def apply(
        typeKey: ReplicatedEntityTypeKey[Command],
        system: ActorSystem[_],
    ): ReplicatedEntity[Command, ReplicationEnvelope[Command]] = {
      val settings =
        if (typeKey == defaultDataStoreTypeKey) {
          ClusterReplicationSettings(system)
        } else {
          ClusterReplicationSettings(system)
            .withRaftJournalPluginId("akka.persistence.journal.proxy")
            .withRaftSnapshotPluginId("akka.persistence.snapshot-store.proxy")
            .withRaftQueryPluginId("lerna.akka.entityreplication.util.persistence.query.proxy")
            .withEventSourcedJournalPluginId("akka.persistence.journal.proxy")
        }
      ReplicatedEntity(typeKey)(entityContext =>
        Behaviors.setup { context =>
          context.setLoggerName(PingPongEntity.getClass)
          ReplicatedEntityBehavior[Command, Event, State](
            entityContext = entityContext,
            emptyState = State(count = 0),
            commandHandler = _ applyCommand _,
            eventHandler = _ applyEvent _,
          )
        },
      ).withSettings(settings)
    }
  }

  class PingPongEventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {

    private[this] val log = Logging(system, getClass)

    override def manifest(event: Any): String = ""

    override def toJournal(event: Any): Any =
      event match {
        case event: PingPongEntity.Event => tagging(event)
        case _ =>
          log.warning("unexpected event: {}", event)
          event // identity
      }

    def tagging(event: PingPongEntity.Event): Tagged =
      event match {
        case event: PingPongEntity.CountUp =>
          Tagged(event, tags = Set("ping-pong"))
      }
  }

}
