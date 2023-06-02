package lerna.akka.entityreplication.rollback.cassandra

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.rollback.RollbackRequirementsNotFulfilled
import lerna.akka.entityreplication.rollback.cassandra.testkit.PersistenceCassandraConfigProvider
import lerna.akka.entityreplication.rollback.testkit.{ CatalogReplicatedEntity, PersistenceInitializationAwaiter }
import lerna.akka.entityreplication.typed._
import lerna.akka.entityreplication.util.AtLeastOnceComplete

import java.time.{ Instant, ZonedDateTime }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.jdk.CollectionConverters._

object CassandraRaftShardRollbackFailureWithDeletionSpecConfig
    extends MultiNodeConfig
    with PersistenceCassandraConfigProvider {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")
  val node4: RoleName = role("node4")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-1"),
    node3 -> MemberIndex("member-2"),
    node4 -> MemberIndex("member-3"),
  )

  // The length of keyspace name should be less than or equal to 48.
  // See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/refLimits.html
  private val journalKeyspaceName  = getClass.getSimpleName.replace("$", "").take(48)
  private val snapshotKeyspaceName = getClass.getSimpleName.replace("$", "").take(48)

  object Rollback {
    val targetShardId: String                = "2"
    val targetLeaderMemberIndex: MemberIndex = MemberIndex("member-1")
  }

  /** Each JVM node should sync its clock in this tolerance */
  val clockOutOfSyncTolerance: FiniteDuration = 500.millis

  private val deletionConfig: Config = ConfigFactory.parseString(
    """
      |lerna.akka.entityreplication.raft {
      |  delete-old-events = on
      |  delete-old-snapshots = on
      |  delete-before-relative-sequence-nr = 200
      |
      |  compaction.log-size-check-interval = 3s
      |  compaction.log-size-threshold = 100
      |  compaction.preserve-log-size = 25
      |
      |  entity-snapshot-store.snapshot-every = 1
      |  entity-snapshot-store.delete-old-events = on
      |  entity-snapshot-store.delete-old-snapshots = on
      |  entity-snapshot-store.delete-before-relative-sequence-nr = 5
      |
      |  eventsourced.persistence.snapshot-every = 100
      |  eventsourced.persistence.delete-old-events = on
      |  eventsourced.persistence.delete-old-snapshots = on
      |  eventsourced.persistence.delete-before-relative-sequence-nr = 200
      |}
      |""".stripMargin,
  )

  commonConfig(
    debugConfig(false)
      .withValue(
        "lerna.akka.entityreplication.raft.multi-raft-roles",
        ConfigValueFactory.fromIterable(memberIndexes.values.map(_.role).toSet.asJava),
      )
      .withFallback(deletionConfig)
      .withFallback(ConfigFactory.parseString(s"""
          |akka.persistence.cassandra.journal {
          |  ${CatalogReplicatedEntity.EventAdapter.config}
          |}
          |""".stripMargin))
      .withFallback(ConfigFactory.load()),
  )

  nodeConfig(node1)(
    ConfigFactory
      .parseString(
        s"""
           |akka.cluster.roles = ["${memberIndexes(node1)}"]
           |lerna.akka.entityreplication.rollback {
           |  clock-out-of-sync-tolerance = ${clockOutOfSyncTolerance.toMillis} ms
           |}
           |""".stripMargin,
      )
      .withFallback(persistenceCassandraConfig(journalKeyspaceName, snapshotKeyspaceName, autoCreate = true)),
  )
  Set(node2, node3, node4).foreach { node =>
    nodeConfig(node)(
      ConfigFactory
        .parseString(
          s"""
             |akka.cluster.roles = ["${memberIndexes(node)}"]
             |""".stripMargin,
        )
        .withFallback(persistenceCassandraConfig(journalKeyspaceName, snapshotKeyspaceName)),
    )
  }

}

final class CassandraRaftShardRollbackFailureWithDeletionSpecMultiJvmNode1
    extends CassandraRaftShardRollbackFailureWithDeletionSpec
final class CassandraRaftShardRollbackFailureWithDeletionSpecMultiJvmNode2
    extends CassandraRaftShardRollbackFailureWithDeletionSpec
final class CassandraRaftShardRollbackFailureWithDeletionSpecMultiJvmNode3
    extends CassandraRaftShardRollbackFailureWithDeletionSpec
final class CassandraRaftShardRollbackFailureWithDeletionSpecMultiJvmNode4
    extends CassandraRaftShardRollbackFailureWithDeletionSpec

class CassandraRaftShardRollbackFailureWithDeletionSpec
    extends MultiNodeSpec(CassandraRaftShardRollbackFailureWithDeletionSpecConfig)
    with STMultiNodeSpec {

  import CassandraRaftShardRollbackFailureWithDeletionSpecConfig._
  import CatalogReplicatedEntity._

  override def initialParticipants: Int = 4

  private implicit val typedSystem: ActorSystem[Nothing] = system.toTyped

  private val targetLeaderMemberIndex: MemberIndex = Rollback.targetLeaderMemberIndex
  private val targetShardId: String                = Rollback.targetShardId
  private val entityIdA                            = "entity-A"
  private val entityIdB                            = "entity-B"
  private val numOfEventsOnRound1                  = 25
  private val numOfEventsOnRound2                  = 300

  private lazy val clusterReplication =
    ClusterReplication(typedSystem)

  private lazy val clusterReplicationSettings =
    ClusterReplicationSettings(typedSystem)
      .withStickyLeaders(Map(targetShardId -> targetLeaderMemberIndex.role))

  private lazy val queries: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  /** Cassandra should be ready to handle requests within this timeout */
  private val cassandraInitializationTimeout: FiniteDuration = 30.seconds

  /** ClusterReplication should be ready to handle requests within this timeout */
  private val initializationTimeout: FiniteDuration = 10.seconds

  /** ClusterReplication should persist events eventually within this timeout. */
  private val propagationTimeout: FiniteDuration = 20.seconds

  // Updated in the following tests
  private var rollbackTimestamp: Option[Instant] = None

  "CassandraRaftShardRollback" should {

    "start Cassandra" in {
      runOn(node1) {
        CassandraLauncher.main(Array(s"$cassandraPort", "true"))
      }
      Thread.sleep(cassandraInitializationTimeout.toMillis)
      enterBarrier("Started Cassandra")
    }

    "initialize Persistence Cassandra" in {
      runOn(node1) {
        PersistenceInitializationAwaiter(system).awaitInit()
      }
      enterBarrier("Initialized Persistence Cassandra (node1)")

      runOn(node2, node3, node4) {
        PersistenceInitializationAwaiter(system).awaitInit()
      }
      enterBarrier("Initialized Persistence Cassandra (nodes: [2,3,4])")
    }

    "form a new cluster (nodes: [2,3,4])" in {
      newCluster(node2, node3, node4)
      enterBarrier("Formed the new cluster (nodes: [2,3,4])")
    }

    "start ClusterReplication (nodes: [2,3,4])" in {
      runOn(node2, node3, node4) {
        clusterReplication.init(CatalogReplicatedEntity().withSettings(clusterReplicationSettings))
      }
      Thread.sleep(initializationTimeout.toMillis)
      enterBarrier("Started ClusterReplication (nodes: [2,3,4])")
    }

    "persist events (round 1)" in {
      runOn(node2) {
        assert(clusterReplication.shardIdOf(typeKey, entityIdA) == targetShardId)
        assert(clusterReplication.shardIdOf(typeKey, entityIdB) != targetShardId)
      }
      implicit val timeout: Timeout = 3000.millis
      runOn(node2) {
        for (i <- 0 until numOfEventsOnRound1) {
          val entityA = clusterReplication.entityRefFor(typeKey, entityIdA)
          AtLeastOnceComplete.askTo(entityA, Add(i, _), 1000.millis).await should be(Done)
        }
      }
      runOn(node3) {
        for (i <- 0 until numOfEventsOnRound1) {
          val entityB = clusterReplication.entityRefFor(typeKey, entityIdB)
          AtLeastOnceComplete.askTo(entityB, Add(i, _), 1000.millis).await should be(Done)
        }
      }
      val totalPersistTimeout =
        clusterReplicationSettings.raftSettings.heartbeatInterval * numOfEventsOnRound1
      enterBarrier(max = totalPersistTimeout, "Persisted Events (round 1)")
    }

    "wait for the completion of the event sourcing (round 1)" in {
      runOn(node2, node3, node4) {
        awaitAssert(
          {
            val source = queries
              .currentEventsByTag(EventAdapter.tag, Offset.noOffset)
            val events = source.runWith(Sink.seq).await.collect {
              case EventEnvelope(_, _, _, event: Event) => event
            }
            val expectedEventsOfEntityA =
              (0 until numOfEventsOnRound1).map(i => Added(entityIdA, i))
            events.filter(_.entityId == entityIdA) should be(expectedEventsOfEntityA)
            val expectedEventsOfEntityB =
              (0 until numOfEventsOnRound1).map(i => Added(entityIdB, i))
            events.filter(_.entityId == entityIdB) should be(expectedEventsOfEntityB)
          },
          max = propagationTimeout,
          interval = 500.millis,
        )
      }
      enterBarrier(max = propagationTimeout, "Completed the event sourcing (round 1)")
    }

    "store a rollback timestamp" in {
      Thread.sleep(clockOutOfSyncTolerance.toMillis)
      runOn(node1) {
        rollbackTimestamp = Option(ZonedDateTime.now().toInstant)
      }
      enterBarrier("Stored the rollback timestamp")
    }

    "persist events (round 2)" in {
      implicit val timeout: Timeout = 3000.millis
      val baseValue                 = numOfEventsOnRound1
      runOn(node2) {
        for (i <- 0 until numOfEventsOnRound2) {
          val entityA = clusterReplication.entityRefFor(typeKey, entityIdA)
          AtLeastOnceComplete.askTo(entityA, Add(baseValue + i, _), 1000.millis).await should be(Done)
        }
      }
      runOn(node3) {
        for (i <- 0 until numOfEventsOnRound2) {
          val entityB = clusterReplication.entityRefFor(typeKey, entityIdB)
          AtLeastOnceComplete.askTo(entityB, Add(baseValue + i, _), 1000.millis).await should be(Done)
        }
      }
      val totalPersistTimeout =
        clusterReplicationSettings.raftSettings.heartbeatInterval * numOfEventsOnRound2
      enterBarrier(max = totalPersistTimeout, "Persisted events (round 2)")
    }

    "wait for the completion of the event sourcing (round 2)" in {
      runOn(node2, node3, node4) {
        awaitAssert(
          {
            val source = queries
              .currentEventsByTag(EventAdapter.tag, Offset.noOffset)
            val events = source.runWith(Sink.seq).await.collect {
              case EventEnvelope(_, _, _, event: Event) => event
            }
            val expectedEventsOfEntityA =
              (0 until numOfEventsOnRound1 + numOfEventsOnRound2).map(i => Added(entityIdA, i))
            events.filter(_.entityId == entityIdA) should be(expectedEventsOfEntityA)
            val expectedEventsOfEntityB =
              (0 until numOfEventsOnRound1 + numOfEventsOnRound2).map(i => Added(entityIdB, i))
            events.filter(_.entityId == entityIdB) should be(expectedEventsOfEntityB)
          },
          max = propagationTimeout,
          interval = 500.millis,
        )
      }
      enterBarrier(max = propagationTimeout, "Completed the event sourcing (round 2)")
    }

    "shut down the cluster (nodes: [2,3,4])" in {
      runOn(node1) {
        testConductor.shutdown(node2).await
        testConductor.shutdown(node3).await
        testConductor.shutdown(node4).await
      }
      runOn(node1) {
        enterBarrier("Shut down the cluster (nodes: [2,3,4])")
      }
    }

    "fail a rollback preparation" in {
      runOn(node1) {
        val rollback    = CassandraRaftShardRollback(system)
        val toTimestamp = rollbackTimestamp.get
        val exception = intercept[RollbackRequirementsNotFulfilled] {
          rollback
            .prepareRollback(
              typeKey.name,
              targetShardId,
              clusterReplicationSettings.raftSettings.multiRaftRoles,
              targetLeaderMemberIndex.role,
              toTimestamp,
            ).await
        }
        log.info("Got expected RollbackRequirementsNotFulfilled: [{}]", exception.getMessage)
      }
    }

  }

}
