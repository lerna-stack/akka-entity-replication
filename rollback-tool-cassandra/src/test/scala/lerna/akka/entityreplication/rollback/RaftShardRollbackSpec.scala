package lerna.akka.entityreplication.rollback

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.rollback.setup._
import lerna.akka.entityreplication.rollback.testkit.PatienceConfigurationForTestKitBase
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.time.{ ZoneOffset, ZonedDateTime }
import scala.concurrent.Future

final class RaftShardRollbackSpec
    extends TestKitBase
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with MockFactory
    with PatienceConfigurationForTestKitBase {

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  private val defaultConfig =
    system.settings.config.getConfig("lerna.akka.entityreplication.rollback")
  private val configForNonDryRun: Config = ConfigFactory
    .parseString("""
      |dry-run = false
      |""".stripMargin)
    .withFallback(defaultConfig)

  private val settings: RaftShardRollbackSettings =
    RaftShardRollbackSettings(configForNonDryRun)
  private val settingsForDryRun: RaftShardRollbackSettings =
    RaftShardRollbackSettings(defaultConfig)

  private val defaultTypeName: TypeName =
    TypeName.from("example")
  private val defaultShardId: NormalizedShardId =
    NormalizedShardId.from("1")

  override def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }

  private def createRaftShardRollback(settings: RaftShardRollbackSettings): RaftShardRollback = {
    new RaftShardRollback(
      system,
      settings,
      new RaftPersistence(
        mock[PersistentActorRollback],
        mock[RaftShardPersistenceQueries],
        mock[SequenceNrSearchStrategy],
        mock[RollbackRequirementsVerifier],
      ),
      new RaftEventSourcedPersistence(
        mock[PersistentActorRollback],
        mock[RollbackRequirementsVerifier],
      ),
    )
  }

  "RaftShardRollback.prepareRaftActorsRollback" should {

    "return all RaftActors' rollback setups for the given Raft shard" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val raftActorId1 = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val raftActorId2 = RaftActorId(defaultTypeName, defaultShardId, memberIndex2)
      val raftActorId3 = RaftActorId(defaultTypeName, defaultShardId, memberIndex3)

      val leadersTimestamp    = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val nonLeadersTimestamp = leadersTimestamp.minusSeconds(rollback.settings.clockOutOfSyncTolerance.toSeconds)

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )

      // Expectations:
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(raftActorId1.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(7))))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(raftActorId2.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(5))))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(raftActorId3.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .once()

      // Expectations:
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(raftActorId1.persistenceId, Some(SequenceNr(7)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(raftActorId2.persistenceId, Some(SequenceNr(5)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(raftActorId3.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()

      // Test:
      rollback.prepareRaftActorsRollback(parameters).futureValue should contain theSameElementsAs Seq(
        RaftActorRollbackSetup(raftActorId1, Some(SequenceNr(7))),
        RaftActorRollbackSetup(raftActorId2, Some(SequenceNr(5))),
        RaftActorRollbackSetup(raftActorId3, None),
      )
    }

    "return a failed Future if the rollback request doesn't meet requirements" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val raftActorId1 = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val raftActorId2 = RaftActorId(defaultTypeName, defaultShardId, memberIndex2)
      val raftActorId3 = RaftActorId(defaultTypeName, defaultShardId, memberIndex3)

      val leadersTimestamp    = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val nonLeadersTimestamp = leadersTimestamp.minusSeconds(rollback.settings.clockOutOfSyncTolerance.toSeconds)

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )

      // Expectations:
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(raftActorId1.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(7))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(raftActorId2.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(5))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(raftActorId3.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .noMoreThanOnce()

      // Expectations:
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(raftActorId1.persistenceId, Some(SequenceNr(7)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(raftActorId2.persistenceId, Some(SequenceNr(5)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(raftActorId3.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.failed(new RollbackRequirementsNotFulfilled("mocked")))
        .once()

      // Test:
      val exception = rollback.prepareRaftActorsRollback(parameters).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
    }

  }

  "RaftShardRollback.prepareSnapshotStoresRollback" should {

    "return all SnapshotStores' rollback setups for the given Raft shard " in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val raftActorId1 = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val raftActorId2 = RaftActorId(defaultTypeName, defaultShardId, memberIndex2)
      val raftActorId3 = RaftActorId(defaultTypeName, defaultShardId, memberIndex3)

      val leadersTimestamp    = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val nonLeadersTimestamp = leadersTimestamp.minusSeconds(rollback.settings.clockOutOfSyncTolerance.toSeconds)

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(raftActorId1, Some(SequenceNr(100))),
        RaftActorRollbackSetup(raftActorId2, Some(SequenceNr(40))),
        RaftActorRollbackSetup(raftActorId3, None),
      )

      val snapshotStoreId1A = SnapshotStoreId(defaultTypeName, memberIndex1, NormalizedEntityId.from("A"))
      val snapshotStoreId1B = SnapshotStoreId(defaultTypeName, memberIndex1, NormalizedEntityId.from("B"))
      val snapshotStoreId2B = SnapshotStoreId(defaultTypeName, memberIndex2, NormalizedEntityId.from("B"))
      val snapshotStoreId2C = SnapshotStoreId(defaultTypeName, memberIndex2, NormalizedEntityId.from("C"))
      val snapshotStoreId3C = SnapshotStoreId(defaultTypeName, memberIndex3, NormalizedEntityId.from("C"))
      val snapshotStoreId3D = SnapshotStoreId(defaultTypeName, memberIndex3, NormalizedEntityId.from("D"))

      // Expectations:
      (rollback.raftPersistence.raftShardPersistenceQueries.entityIdsAfter _)
        .expects(raftActorId1, SequenceNr(100))
        .returns(
          Source(
            Seq(
              snapshotStoreId1A.entityId,
              snapshotStoreId1B.entityId,
              snapshotStoreId1A.entityId,
            ),
          ),
        )
        .once()
      (rollback.raftPersistence.raftShardPersistenceQueries.entityIdsAfter _)
        .expects(raftActorId2, SequenceNr(40))
        .returns(
          Source(
            Seq(
              snapshotStoreId2B.entityId,
              snapshotStoreId2B.entityId,
              snapshotStoreId2C.entityId,
            ),
          ),
        )
        .once()
      (rollback.raftPersistence.raftShardPersistenceQueries.entityIdsAfter _)
        .expects(raftActorId3, SequenceNr(1))
        .returns(
          Source(
            Seq(
              snapshotStoreId3D.entityId,
              snapshotStoreId3C.entityId,
              snapshotStoreId3C.entityId,
            ),
          ),
        )
        .once()

      // Expectations:
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId1A.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(2))))
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId1B.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(5))))
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId2B.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(3))))
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId2C.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId3C.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(1))))
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId3D.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))

      // Expectations:
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId1A.persistenceId, Some(SequenceNr(2)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId1B.persistenceId, Some(SequenceNr(5)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId2B.persistenceId, Some(SequenceNr(3)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId2C.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId3C.persistenceId, Some(SequenceNr(1)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId3D.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()

      // Test:
      val snapshotStoresRollbackSetups =
        rollback.prepareSnapshotStoresRollback(parameters, raftActorRollbackSetups).futureValue
      snapshotStoresRollbackSetups should contain theSameElementsAs Seq(
        SnapshotStoreRollbackSetup(snapshotStoreId1A, Some(SequenceNr(2))),
        SnapshotStoreRollbackSetup(snapshotStoreId1B, Some(SequenceNr(5))),
        SnapshotStoreRollbackSetup(snapshotStoreId2B, Some(SequenceNr(3))),
        SnapshotStoreRollbackSetup(snapshotStoreId2C, None),
        SnapshotStoreRollbackSetup(snapshotStoreId3C, Some(SequenceNr(1))),
        SnapshotStoreRollbackSetup(snapshotStoreId3D, None),
      )
    }

    "return a failed Future if the rollback request doesn't meet requirements" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val raftActorId1 = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val raftActorId2 = RaftActorId(defaultTypeName, defaultShardId, memberIndex2)
      val raftActorId3 = RaftActorId(defaultTypeName, defaultShardId, memberIndex3)

      val leadersTimestamp    = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val nonLeadersTimestamp = leadersTimestamp.minusSeconds(rollback.settings.clockOutOfSyncTolerance.toSeconds)

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(raftActorId1, Some(SequenceNr(100))),
        RaftActorRollbackSetup(raftActorId2, Some(SequenceNr(40))),
        RaftActorRollbackSetup(raftActorId3, None),
      )

      val snapshotStoreId1A = SnapshotStoreId(defaultTypeName, memberIndex1, NormalizedEntityId.from("A"))
      val snapshotStoreId1B = SnapshotStoreId(defaultTypeName, memberIndex1, NormalizedEntityId.from("B"))
      val snapshotStoreId2B = SnapshotStoreId(defaultTypeName, memberIndex2, NormalizedEntityId.from("B"))
      val snapshotStoreId2C = SnapshotStoreId(defaultTypeName, memberIndex2, NormalizedEntityId.from("C"))
      val snapshotStoreId3C = SnapshotStoreId(defaultTypeName, memberIndex3, NormalizedEntityId.from("C"))
      val snapshotStoreId3D = SnapshotStoreId(defaultTypeName, memberIndex3, NormalizedEntityId.from("D"))

      // Expectations:
      (rollback.raftPersistence.raftShardPersistenceQueries.entityIdsAfter _)
        .expects(raftActorId1, SequenceNr(100))
        .returns(
          Source(
            Seq(
              snapshotStoreId1A.entityId,
              snapshotStoreId1B.entityId,
              snapshotStoreId1A.entityId,
            ),
          ),
        )
        .noMoreThanOnce()
      (rollback.raftPersistence.raftShardPersistenceQueries.entityIdsAfter _)
        .expects(raftActorId2, SequenceNr(40))
        .returns(
          Source(
            Seq(
              snapshotStoreId2B.entityId,
              snapshotStoreId2B.entityId,
              snapshotStoreId2C.entityId,
            ),
          ),
        )
        .noMoreThanOnce()
      (rollback.raftPersistence.raftShardPersistenceQueries.entityIdsAfter _)
        .expects(raftActorId3, SequenceNr(1))
        .returns(
          Source(
            Seq(
              snapshotStoreId3D.entityId,
              snapshotStoreId3C.entityId,
              snapshotStoreId3C.entityId,
            ),
          ),
        )
        .noMoreThanOnce()

      // Expectations:
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId1A.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(2))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId1B.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(5))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId2B.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(3))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId2C.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId3C.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(1))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotStoreId3D.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .noMoreThanOnce()

      // Expectations:
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId1A.persistenceId, Some(SequenceNr(2)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId1B.persistenceId, Some(SequenceNr(5)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId2B.persistenceId, Some(SequenceNr(3)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId2C.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId3C.persistenceId, Some(SequenceNr(1)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotStoreId3D.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.failed(new RollbackRequirementsNotFulfilled("mocked")))
        .once()

      // Test:
      val exception = rollback.prepareSnapshotStoresRollback(parameters, raftActorRollbackSetups).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
    }

  }

  "RaftShardRollback.prepareSnapshotSyncManagersRollback" should {

    "return all SnapshotSyncManagers' rollback setups for the given Raft shard" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp    = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val nonLeadersTimestamp = leadersTimestamp.minusSeconds(rollback.settings.clockOutOfSyncTolerance.toSeconds)

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )

      val snapshotSyncManagerId21 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex2, memberIndex1)
      val snapshotSyncManagerId31 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex3, memberIndex1)
      val snapshotSyncManagerId12 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex1, memberIndex2)
      val snapshotSyncManagerId32 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex3, memberIndex2)
      val snapshotSyncManagerId13 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex1, memberIndex3)
      val snapshotSyncManagerId23 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex2, memberIndex3)

      // Expectations:
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId21.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(7))))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId31.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(10))))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId12.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId32.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(20))))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId13.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(30))))
        .once()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId23.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .once()

      // Expectations:
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId21.persistenceId, Some(SequenceNr(7)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId31.persistenceId, Some(SequenceNr(10)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId12.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId32.persistenceId, Some(SequenceNr(20)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId13.persistenceId, Some(SequenceNr(30)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId23.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .once()

      // Test:
      rollback.prepareSnapshotSyncManagersRollback(parameters).futureValue should contain theSameElementsAs Seq(
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId21, Some(SequenceNr(7))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId31, Some(SequenceNr(10))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId12, None),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId32, Some(SequenceNr(20))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId13, Some(SequenceNr(30))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId23, None),
      )
    }

    "return a failed Future if the rollback request doesn't meet requirements" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp    = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val nonLeadersTimestamp = leadersTimestamp.minusSeconds(rollback.settings.clockOutOfSyncTolerance.toSeconds)

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )

      val snapshotSyncManagerId21 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex2, memberIndex1)
      val snapshotSyncManagerId31 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex3, memberIndex1)
      val snapshotSyncManagerId12 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex1, memberIndex2)
      val snapshotSyncManagerId32 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex3, memberIndex2)
      val snapshotSyncManagerId13 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex1, memberIndex3)
      val snapshotSyncManagerId23 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex2, memberIndex3)

      // Expectations:
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId21.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(7))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId31.persistenceId, leadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(10))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId12.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId32.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(20))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId13.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(Some(SequenceNr(30))))
        .noMoreThanOnce()
      (rollback.raftPersistence.sequenceNrSearchStrategy.findUpperBound _)
        .expects(snapshotSyncManagerId23.persistenceId, nonLeadersTimestamp)
        .returns(Future.successful(None))
        .noMoreThanOnce()

      // Expectations:
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId21.persistenceId, Some(SequenceNr(7)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId31.persistenceId, Some(SequenceNr(10)), Some(leadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId12.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId32.persistenceId, Some(SequenceNr(20)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId13.persistenceId, Some(SequenceNr(30)), Some(nonLeadersTimestamp))
        .returns(Future.successful(Done))
        .noMoreThanOnce()
      (rollback.raftPersistence.requirementsVerifier.verify _)
        .expects(snapshotSyncManagerId23.persistenceId, None, Some(nonLeadersTimestamp))
        .returns(Future.failed(new RollbackRequirementsNotFulfilled("mocked")))
        .once()

      // Test:
      val exception = rollback.prepareSnapshotSyncManagersRollback(parameters).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
    }

  }

  "RaftShardRollback.prepareCommitLogStoreActorRollback" should {

    "throw an IllegalArgumentException if the given RaftActors' setups doesn't contain the leader's setup" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp =
        ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val invalidRaftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex2), Some(SequenceNr(4))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex3), Some(SequenceNr(5))),
      )

      // Test:
      intercept[IllegalArgumentException] {
        rollback.prepareCommitLogStoreActorRollback(parameters, invalidRaftActorRollbackSetups)
      }
    }

    "return a CommitLogStoreActor's rollback setup for the given Raft shard" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp =
        ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex1), Some(SequenceNr(10))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex2), Some(SequenceNr(4))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex3), Some(SequenceNr(5))),
      )

      val leaderRaftActorId     = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val commitLogStoreActorId = CommitLogStoreActorId(defaultTypeName, defaultShardId)

      // Expectations:
      (rollback.raftPersistence.raftShardPersistenceQueries
        .findLastTruncatedLogEntryIndex(_: RaftActorId, _: SequenceNr)(_: Materializer))
        .expects(leaderRaftActorId, SequenceNr(10), *)
        .returns(Future.successful(Some(LogEntryIndex(2))))
        .once()

      // Expectations:
      (rollback.raftEventSourcedPersistence.requirementsVerifier.verify _)
        .expects(commitLogStoreActorId.persistenceId, Some(SequenceNr(2)), None)
        .returns(Future.successful(Done))
        .once()

      // Test:
      rollback.prepareCommitLogStoreActorRollback(parameters, raftActorRollbackSetups).futureValue should be(
        CommitLogStoreActorRollbackSetup(commitLogStoreActorId, Some(SequenceNr(2))),
      )
    }

    "return a CommitLogStoreActor's rollback setup (indicates all data delete) for the given Raft shard" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp =
        ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex1), Some(SequenceNr(10))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex2), Some(SequenceNr(4))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex3), Some(SequenceNr(5))),
      )

      val leaderRaftActorId     = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val commitLogStoreActorId = CommitLogStoreActorId(defaultTypeName, defaultShardId)

      // Expectations:
      (rollback.raftPersistence.raftShardPersistenceQueries
        .findLastTruncatedLogEntryIndex(_: RaftActorId, _: SequenceNr)(_: Materializer))
        .expects(leaderRaftActorId, SequenceNr(10), *)
        .returns(Future.successful(None))
        .once()

      // Expectations:
      (rollback.raftEventSourcedPersistence.requirementsVerifier.verify _)
        .expects(commitLogStoreActorId.persistenceId, None, None)
        .returns(Future.successful(Done))
        .once()

      // Test:
      rollback.prepareCommitLogStoreActorRollback(parameters, raftActorRollbackSetups).futureValue should be(
        CommitLogStoreActorRollbackSetup(commitLogStoreActorId, None),
      )
    }

    "return a CommitLogStoreActor's rollback setup (indicates all data delete) for the given Raft shard" +
    " if the leader's rollback setup indicates deleting all data" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp =
        ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex1), None),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex2), Some(SequenceNr(4))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex3), Some(SequenceNr(5))),
      )

      val commitLogStoreActorId = CommitLogStoreActorId(defaultTypeName, defaultShardId)

      // Expectations:
      (rollback.raftEventSourcedPersistence.requirementsVerifier.verify _)
        .expects(commitLogStoreActorId.persistenceId, None, None)
        .returns(Future.successful(Done))
        .once()

      // Test:
      rollback.prepareCommitLogStoreActorRollback(parameters, raftActorRollbackSetups).futureValue should be(
        CommitLogStoreActorRollbackSetup(commitLogStoreActorId, None),
      )
    }

    "return a failed Future if the rollback request doesn't meet requirements" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      val leadersTimestamp =
        ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

      val parameters =
        RaftShardRollbackParameters(
          defaultTypeName,
          defaultShardId,
          Set(memberIndex1, memberIndex2, memberIndex3),
          memberIndex1,
          leadersTimestamp,
        )
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex1), Some(SequenceNr(10))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex2), Some(SequenceNr(4))),
        RaftActorRollbackSetup(RaftActorId(defaultTypeName, defaultShardId, memberIndex3), Some(SequenceNr(5))),
      )

      val leaderRaftActorId     = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val commitLogStoreActorId = CommitLogStoreActorId(defaultTypeName, defaultShardId)

      // Expectations:
      (rollback.raftPersistence.raftShardPersistenceQueries
        .findLastTruncatedLogEntryIndex(_: RaftActorId, _: SequenceNr)(_: Materializer))
        .expects(leaderRaftActorId, SequenceNr(10), *)
        .returns(Future.successful(None))
        .once()

      // Expectations:
      (rollback.raftEventSourcedPersistence.requirementsVerifier.verify _)
        .expects(commitLogStoreActorId.persistenceId, None, None)
        .returns(Future.failed(new RollbackRequirementsNotFulfilled("mocked")))
        .once()

      // Test:
      val exception =
        rollback.prepareCommitLogStoreActorRollback(parameters, raftActorRollbackSetups).failed.futureValue
      exception should be(a[RollbackRequirementsNotFulfilled])
    }

  }

  "RaftShardRollback.rollback" should {

    "call all rollback operations for the given rollback setup" in {
      val rollback = createRaftShardRollback(settings)

      val memberIndex1 = MemberIndex("replica-group-1")
      val memberIndex2 = MemberIndex("replica-group-2")
      val memberIndex3 = MemberIndex("replica-group-3")

      // RaftActors' rollback setups
      val raftActorId1 = RaftActorId(defaultTypeName, defaultShardId, memberIndex1)
      val raftActorId2 = RaftActorId(defaultTypeName, defaultShardId, memberIndex2)
      val raftActorId3 = RaftActorId(defaultTypeName, defaultShardId, memberIndex3)
      val raftActorRollbackSetups = Seq(
        RaftActorRollbackSetup(raftActorId1, Some(SequenceNr(100))),
        RaftActorRollbackSetup(raftActorId2, Some(SequenceNr(200))),
        RaftActorRollbackSetup(raftActorId3, None),
      )

      // SnapshotStores' rollback setups
      val snapshotStoreId1A = SnapshotStoreId(defaultTypeName, memberIndex1, NormalizedEntityId.from("A"))
      val snapshotStoreId1B = SnapshotStoreId(defaultTypeName, memberIndex1, NormalizedEntityId.from("B"))
      val snapshotStoreId2B = SnapshotStoreId(defaultTypeName, memberIndex2, NormalizedEntityId.from("B"))
      val snapshotStoreId2C = SnapshotStoreId(defaultTypeName, memberIndex2, NormalizedEntityId.from("C"))
      val snapshotStoreId3C = SnapshotStoreId(defaultTypeName, memberIndex3, NormalizedEntityId.from("C"))
      val snapshotStoreId3D = SnapshotStoreId(defaultTypeName, memberIndex3, NormalizedEntityId.from("D"))
      val snapshotStoreRollbackSetups = Seq(
        SnapshotStoreRollbackSetup(snapshotStoreId1A, Some(SequenceNr(20))),
        SnapshotStoreRollbackSetup(snapshotStoreId1B, Some(SequenceNr(50))),
        SnapshotStoreRollbackSetup(snapshotStoreId2B, Some(SequenceNr(30))),
        SnapshotStoreRollbackSetup(snapshotStoreId2C, None),
        SnapshotStoreRollbackSetup(snapshotStoreId3C, Some(SequenceNr(10))),
        SnapshotStoreRollbackSetup(snapshotStoreId3D, None),
      )

      // SnapshotSyncManagers' rollback setups
      val snapshotSyncManagerId21 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex2, memberIndex1)
      val snapshotSyncManagerId31 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex3, memberIndex1)
      val snapshotSyncManagerId12 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex1, memberIndex2)
      val snapshotSyncManagerId32 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex3, memberIndex2)
      val snapshotSyncManagerId13 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex1, memberIndex3)
      val snapshotSyncManagerId23 = SnapshotSyncManagerId(defaultTypeName, defaultShardId, memberIndex2, memberIndex3)
      val snapshotSyncManagerRollbackSetups = Seq(
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId21, Some(SequenceNr(60))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId31, Some(SequenceNr(40))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId12, None),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId32, Some(SequenceNr(55))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId13, Some(SequenceNr(35))),
        SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId23, None),
      )

      // CommitLogStoreActor's rollback setups
      val commitLogStoreActorId       = CommitLogStoreActorId(defaultTypeName, defaultShardId)
      val commitLogStoreRollbackSetup = CommitLogStoreActorRollbackSetup(commitLogStoreActorId, Some(SequenceNr(80)))

      val raftShardRollbackSetup = RaftShardRollbackSetup(
        raftActorRollbackSetups,
        snapshotStoreRollbackSetups,
        snapshotSyncManagerRollbackSetups,
        commitLogStoreRollbackSetup,
      )

      // Expectations: dry-run
      (() => rollback.raftPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(false)
        .atLeastOnce()
      (() => rollback.raftEventSourcedPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(false)
        .atLeastOnce()

      // Expectations: RaftActors' rollbacks
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(raftActorId1.persistenceId, SequenceNr(100))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(raftActorId2.persistenceId, SequenceNr(200))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.deleteAll _)
        .expects(raftActorId3.persistenceId)
        .returns(Future.successful(Done))
        .once()

      // Expectations: SnapshotStores' rollbacks
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotStoreId1A.persistenceId, SequenceNr(20))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotStoreId1B.persistenceId, SequenceNr(50))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotStoreId2B.persistenceId, SequenceNr(30))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.deleteAll _)
        .expects(snapshotStoreId2C.persistenceId)
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotStoreId3C.persistenceId, SequenceNr(10))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.deleteAll _)
        .expects(snapshotStoreId3D.persistenceId)
        .returns(Future.successful(Done))
        .once()

      // Expectations: SnapshotSyncManagers' rollbacks
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotSyncManagerId21.persistenceId, SequenceNr(60))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotSyncManagerId31.persistenceId, SequenceNr(40))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.deleteAll _)
        .expects(snapshotSyncManagerId12.persistenceId)
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotSyncManagerId32.persistenceId, SequenceNr(55))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.rollbackTo _)
        .expects(snapshotSyncManagerId13.persistenceId, SequenceNr(35))
        .returns(Future.successful(Done))
        .once()
      (rollback.raftPersistence.persistentActorRollback.deleteAll _)
        .expects(snapshotSyncManagerId23.persistenceId)
        .returns(Future.successful(Done))
        .once()

      // Expectations: CommitLogStore's rollback
      (rollback.raftEventSourcedPersistence.persistentActorRollback.rollbackTo _)
        .expects(commitLogStoreActorId.persistenceId, SequenceNr(80))
        .returns(Future.successful(Done))
        .once()

      // Test:
      rollback.rollback(raftShardRollbackSetup).futureValue should be(Done)
    }

    "throw an AssertionError if it runs in dry-run mode and the raft-persistence runs not in dry-run mode" in {
      val rollback = createRaftShardRollback(settingsForDryRun)

      (() => rollback.raftPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(false)
        .atLeastOnce()
      (() => rollback.raftEventSourcedPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(true)
        .anyNumberOfTimes()

      val error = intercept[AssertionError] {
        rollback.rollback(new RaftShardRollbackSetup(Seq.empty, Seq.empty))
      }
      error.getMessage should be(
        "assertion failed: The underlying rollback for raft-persistence " +
        "should have dry-run mode [false] the same as this dry-run mode [true]",
      )
    }

    "throw an AssertionError if it runs in dry-run mode and the raft-eventsourced-persistence runs not in dry-run mode" in {
      val rollback = createRaftShardRollback(settingsForDryRun)

      (() => rollback.raftPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(true)
        .anyNumberOfTimes()
      (() => rollback.raftEventSourcedPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(false)
        .atLeastOnce()

      val error = intercept[AssertionError] {
        rollback.rollback(new RaftShardRollbackSetup(Seq.empty, Seq.empty))
      }
      error.getMessage should be(
        "assertion failed: The underlying rollback for raft-eventsourced-persistence " +
        "should have dry-run mode [false] the same as this dry-run mode [true]",
      )
    }

    "throw an AssertionError if it runs not in dry-run mode and the raft-persistence runs in dry-run mode" in {
      val rollback = createRaftShardRollback(settings)

      (() => rollback.raftPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(true)
        .atLeastOnce()
      (() => rollback.raftEventSourcedPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(false)
        .anyNumberOfTimes()

      val error = intercept[AssertionError] {
        rollback.rollback(new RaftShardRollbackSetup(Seq.empty, Seq.empty))
      }
      error.getMessage should be(
        "assertion failed: The underlying rollback for raft-persistence " +
        "should have dry-run mode [true] the same as this dry-run mode [false]",
      )
    }

    "throw an AssertionError if it runs not in dry-run mode and the raft-eventsourced-persistence runs in dry-run mode" in {
      val rollback: RaftShardRollback = createRaftShardRollback(settings)

      (() => rollback.raftPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(false)
        .anyNumberOfTimes()
      (() => rollback.raftEventSourcedPersistence.persistentActorRollback.isDryRun)
        .expects()
        .returns(true)
        .atLeastOnce()

      val error = intercept[AssertionError] {
        rollback.rollback(new RaftShardRollbackSetup(Seq.empty, Seq.empty))
      }
      error.getMessage should be(
        "assertion failed: The underlying rollback for raft-eventsourced-persistence " +
        "should have dry-run mode [true] the same as this dry-run mode [false]",
      )
    }

  }

}
