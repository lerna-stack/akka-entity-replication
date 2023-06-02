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
import lerna.akka.entityreplication.rollback.cassandra.testkit.PersistenceCassandraConfigProvider
import lerna.akka.entityreplication.rollback.testkit.{ CatalogReplicatedEntity, PersistenceInitializationAwaiter }
import lerna.akka.entityreplication.typed._
import lerna.akka.entityreplication.util.AtLeastOnceComplete

import java.time.{ Instant, ZonedDateTime }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.jdk.CollectionConverters._

object CassandraRaftShardRollbackWithDeletionSpecConfig
    extends MultiNodeConfig
    with PersistenceCassandraConfigProvider {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")
  val node4: RoleName = role("node4")
  val node5: RoleName = role("node5")
  val node6: RoleName = role("node6")
  val node7: RoleName = role("node7")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-1"),
    node3 -> MemberIndex("member-2"),
    node4 -> MemberIndex("member-3"),
    node5 -> MemberIndex("member-1"),
    node6 -> MemberIndex("member-2"),
    node7 -> MemberIndex("member-3"),
  )

  private val journalKeyspaceName  = getClass.getSimpleName.replace("$", "")
  private val snapshotKeyspaceName = getClass.getSimpleName.replace("$", "")

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
           |  dry-run = false
           |  log-progress-every = 5
           |  clock-out-of-sync-tolerance = ${clockOutOfSyncTolerance.toMillis} ms
           |}
           |""".stripMargin,
      )
      .withFallback(persistenceCassandraConfig(journalKeyspaceName, snapshotKeyspaceName, autoCreate = true)),
  )
  Set(node2, node3, node4, node5, node6, node7).foreach { node =>
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

final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode1 extends CassandraRaftShardRollbackWithDeletionSpec
final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode2 extends CassandraRaftShardRollbackWithDeletionSpec
final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode3 extends CassandraRaftShardRollbackWithDeletionSpec
final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode4 extends CassandraRaftShardRollbackWithDeletionSpec
final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode5 extends CassandraRaftShardRollbackWithDeletionSpec
final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode6 extends CassandraRaftShardRollbackWithDeletionSpec
final class CassandraRaftShardRollbackWithDeletionSpecMultiJvmNode7 extends CassandraRaftShardRollbackWithDeletionSpec

class CassandraRaftShardRollbackWithDeletionSpec
    extends MultiNodeSpec(CassandraRaftShardRollbackWithDeletionSpecConfig)
    with STMultiNodeSpec {

  import CassandraRaftShardRollbackWithDeletionSpecConfig._
  import CatalogReplicatedEntity._

  override def initialParticipants: Int = 7

  private implicit val typedSystem: ActorSystem[Nothing] = system.toTyped

  private val targetLeaderMemberIndex: MemberIndex = Rollback.targetLeaderMemberIndex
  private val targetShardId: String                = Rollback.targetShardId
  private val entityIdA                            = "entity-A"
  private val entityIdB                            = "entity-B"
  private val numOfEventsOnRound1                  = 300
  private val numOfEventsOnRound2                  = 25
  private val numOfEventsOnRound3                  = 25

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

  /** Rollback tool should complete a rollback within this timeout */
  private val rollbackTimeout: FiniteDuration = 30.seconds

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

      runOn(node2, node3, node4, node5, node6, node7) {
        PersistenceInitializationAwaiter(system).awaitInit()
      }
      enterBarrier("Initialized Persistence Cassandra (nodes: [2,3,4,5,6,7])")
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
        log.info("Node [{}] stored timestamp [{}] for rollback", node1, rollbackTimestamp)
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
      runOn(node1, node5, node6, node7) {
        enterBarrier("Shut down the cluster (nodes: [2,3,4])")
      }
    }

    "roll back the target shard" in {
      runOn(node1) {
        val rollback    = CassandraRaftShardRollback(system)
        val toTimestamp = rollbackTimestamp.get
        val rollbackSetup = rollback
          .prepareRollback(
            typeKey.name,
            targetShardId,
            clusterReplicationSettings.raftSettings.multiRaftRoles,
            targetLeaderMemberIndex.role,
            toTimestamp,
          ).await
        rollback.rollback(rollbackSetup).await should be(Done)
      }
      runOn(node1, node5, node6, node7) {
        enterBarrier(max = rollbackTimeout, "rolled back the target shard")
      }
    }

    "form a new cluster (nodes: [5,6,7])" in {
      newCluster(node5, node6, node7)
      runOn(node1, node5, node6, node7) {
        enterBarrier("Formed the new cluster (nodes: [5,6,7])")
      }
    }

    "start ClusterReplication (nodes: [5,6,7])" in {
      runOn(node5, node6, node7) {
        clusterReplication.init(CatalogReplicatedEntity().withSettings(clusterReplicationSettings))
      }
      runOn(node1, node5, node6, node7) {
        Thread.sleep(initializationTimeout.toMillis)
        enterBarrier("Started ClusterReplication (nodes: [5,6,7])")
      }
    }

    "read the rolled-back data from entities" in {
      runOn(node5, node6, node7) {
        implicit val timeout: Timeout = 3000.millis
        awaitAssert(
          {
            val entityA  = clusterReplication.entityRefFor(typeKey, entityIdA)
            val response = AtLeastOnceComplete.askTo(entityA, Get(_), 1000.millis).await
            response.values should contain theSameElementsAs (0 until numOfEventsOnRound1)
          },
          max = initializationTimeout,
          interval = 500.millis,
        )
        awaitAssert(
          {
            val entityB  = clusterReplication.entityRefFor(typeKey, entityIdB)
            val response = AtLeastOnceComplete.askTo(entityB, Get(_), 1000.millis).await
            response.values should contain theSameElementsAs (0 until numOfEventsOnRound1 + numOfEventsOnRound2)
          },
          max = initializationTimeout,
          interval = 500.millis,
        )
      }
      runOn(node1, node5, node6, node7) {
        enterBarrier(max = initializationTimeout * 2, "Read the rolled-back data from entity")
      }
    }

    "read the rolled-back data via tag queries" in {
      runOn(node5, node6, node7) {
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
              (0 until numOfEventsOnRound1 + numOfEventsOnRound2).map(i => Added(entityIdB, i))
            events.filter(_.entityId == entityIdB) should be(expectedEventsOfEntityB)
          },
          max = propagationTimeout,
          interval = 500.millis,
        )
      }
      runOn(node1, node5, node6, node7) {
        enterBarrier(max = propagationTimeout, "Read the rolled-back data via tag queries")
      }
    }

    "persist events (round 3) after the rollback" in {
      implicit val timeout: Timeout = 3000.millis
      val baseValue                 = numOfEventsOnRound1 + numOfEventsOnRound2
      runOn(node5) {
        for (i <- 0 until numOfEventsOnRound3) {
          val entityA = clusterReplication.entityRefFor(typeKey, entityIdA)
          AtLeastOnceComplete.askTo(entityA, Add(baseValue + i, _), 1000.millis).await should be(Done)
        }
      }
      runOn(node6) {
        for (i <- 0 until numOfEventsOnRound3) {
          val entityB = clusterReplication.entityRefFor(typeKey, entityIdB)
          AtLeastOnceComplete.askTo(entityB, Add(baseValue + i, _), 1000.millis).await should be(Done)
        }
      }
      val totalPersistTimeout =
        clusterReplicationSettings.raftSettings.heartbeatInterval * numOfEventsOnRound3
      runOn(node1, node5, node6, node7) {
        enterBarrier(max = totalPersistTimeout, "Persisted events (round 3)")
      }
    }

    "read the newly persisted data after the rollback from entities" in {
      runOn(node5, node6, node7) {
        implicit val timeout: Timeout = 3000.millis
        awaitAssert(
          {
            val entityA  = clusterReplication.entityRefFor(typeKey, entityIdA)
            val response = AtLeastOnceComplete.askTo(entityA, Get(_), 1000.millis).await
            val expectedValuesOfEntityA =
              (0 until numOfEventsOnRound1) ++
              (0 until numOfEventsOnRound3).map(numOfEventsOnRound1 + numOfEventsOnRound2 + _)
            response.values should contain theSameElementsAs expectedValuesOfEntityA
          },
          max = initializationTimeout,
          interval = 500.millis,
        )
        awaitAssert(
          {
            val entityB  = clusterReplication.entityRefFor(typeKey, entityIdB)
            val response = AtLeastOnceComplete.askTo(entityB, Get(_), 1000.millis).await
            response.values should contain theSameElementsAs (0 until numOfEventsOnRound1 + numOfEventsOnRound2 + numOfEventsOnRound3)
          },
          max = initializationTimeout,
          interval = 500.millis,
        )
      }
      runOn(node1, node5, node6, node7) {
        enterBarrier(max = initializationTimeout * 2, "Read the newly persisted data after the rollback from entities")
      }
    }

    "wait for the completion of the event sourcing that provides tagged events persisted after the rollback" in {
      runOn(node5, node6, node7) {
        awaitAssert(
          {
            val source = queries
              .currentEventsByTag(EventAdapter.tag, Offset.noOffset)
            val events = source.runWith(Sink.seq).await.collect {
              case EventEnvelope(_, _, _, event: Event) => event
            }
            val expectedEventsOfEntityA =
              (0 until numOfEventsOnRound1).map(i => Added(entityIdA, i)) ++
              (0 until numOfEventsOnRound3).map(i => Added(entityIdA, numOfEventsOnRound1 + numOfEventsOnRound2 + i))
            events.filter(_.entityId == entityIdA) should be(expectedEventsOfEntityA)
            val expectedEventsOfEntityB =
              (0 until numOfEventsOnRound1 + numOfEventsOnRound2 + numOfEventsOnRound3).map(i => Added(entityIdB, i))
            events.filter(_.entityId == entityIdB) should be(expectedEventsOfEntityB)
          },
          max = propagationTimeout,
          interval = 500.millis,
        )
      }
      runOn(node1, node5, node6, node7) {
        enterBarrier(max = propagationTimeout, "Read tagged events persisted after the rollback via tag queries")
      }
    }

  }

}
