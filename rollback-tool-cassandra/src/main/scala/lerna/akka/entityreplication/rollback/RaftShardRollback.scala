package lerna.akka.entityreplication.rollback

import akka.Done
import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.scaladsl.{ Sink, Source }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.rollback.setup._

import java.time.Instant
import scala.concurrent.Future

/** Provides rolling back the Raft shard to the specific timestamp */
private[rollback] final class RaftShardRollback(
    systemProvider: ClassicActorSystemProvider,
    val settings: RaftShardRollbackSettings,
    val raftPersistence: RaftPersistence,
    val raftEventSourcedPersistence: RaftEventSourcedPersistence,
) {

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  private val log: LoggingAdapter =
    Logging(system, classOf[RaftShardRollback])

  import system.dispatcher

  /** Calculates and returns a rollback setup for the given Raft shard
    *
    * Note that this method doesn't execute actual rollback. [[rollback]] takes the rollback setup returned by this
    * method and then executes the actual rollback. The setup can be used to review how rollback is executed.
    *
    * @see [[rollback]]
    */
  def prepareRollback(parameters: RaftShardRollbackParameters): Future[RaftShardRollbackSetup] = {
    for {
      raftActorRollbackSetups           <- prepareRaftActorsRollback(parameters)
      snapshotStoreRollbackSetups       <- prepareSnapshotStoresRollback(parameters, raftActorRollbackSetups)
      snapshotSyncManagerRollbackSetups <- prepareSnapshotSyncManagersRollback(parameters)
      commitLogStoreActorRollbackSetup  <- prepareCommitLogStoreActorRollback(parameters, raftActorRollbackSetups)
    } yield {
      RaftShardRollbackSetup(
        raftActorRollbackSetups,
        snapshotStoreRollbackSetups,
        snapshotSyncManagerRollbackSetups,
        commitLogStoreActorRollbackSetup,
      )
    }
  }

  /** Calculates and returns RaftActors' rollback setups for the given Raft shard
    *
    * @see [[prepareRollback]]
    */
  def prepareRaftActorsRollback(
      parameters: RaftShardRollbackParameters,
  ): Future[Seq[RaftActorRollbackSetup]] = {
    Source(parameters.allMemberIndices)
      .mapAsyncUnordered(settings.readParallelism) { memberIndex =>
        val raftActorId = RaftActorId(parameters.typeName, parameters.shardId, memberIndex)
        val toTimestamp = toTimestampFor(parameters, memberIndex)
        raftPersistence.sequenceNrSearchStrategy
          .findUpperBound(raftActorId.persistenceId, toTimestamp)
          .map { sequenceNr =>
            log.info(
              "Calculated RaftActor rollback setup: persistence_id=[{}], to_sequence_nr=[{}]",
              raftActorId.persistenceId,
              sequenceNr.fold(0L)(_.value),
            )
            RaftActorRollbackSetup(raftActorId, sequenceNr)
          }
      }
      .runWith(Sink.seq)
  }

  /** Calculates and returns SnapshotStores' rollback setups for the given Raft shard
    *
    * @see [[prepareRollback]]
    */
  def prepareSnapshotStoresRollback(
      parameters: RaftShardRollbackParameters,
      raftActorRollbackSetups: Seq[RaftActorRollbackSetup],
  ): Future[Seq[SnapshotStoreRollbackSetup]] = {
    val snapshotStoreIds =
      Source(raftActorRollbackSetups)
        .flatMapMerge(
          settings.readParallelism,
          setup => {
            val from = setup.to.getOrElse(SequenceNr(1))
            raftPersistence.raftShardPersistenceQueries
              .entityIdsAfter(setup.id, from)
              .map(entityId => SnapshotStoreId(setup.id.typeName, setup.id.memberIndex, entityId))
          },
        ).runWith(Sink.seq)
        .map(_.toSet)
    Source
      .futureSource(snapshotStoreIds.map(Source(_)))
      .mapAsyncUnordered(settings.readParallelism) { snapshotStoreId =>
        val toTimestamp = toTimestampFor(parameters, snapshotStoreId.memberIndex)
        raftPersistence.sequenceNrSearchStrategy
          .findUpperBound(snapshotStoreId.persistenceId, toTimestamp)
          .map { sequenceNr =>
            log.info(
              "Calculated SnapshotStore rollback setup: persistence_id=[{}], to_sequence_nr=[{}]",
              snapshotStoreId.persistenceId,
              sequenceNr.fold(0L)(_.value),
            )
            SnapshotStoreRollbackSetup(snapshotStoreId, sequenceNr)
          }
      }
      .runWith(Sink.seq)
  }

  /** Calculates and returns SnapshotSyncManagers' rollback setups for the given Raft shard
    *
    * @see [[prepareRollback]]
    */
  def prepareSnapshotSyncManagersRollback(
      parameters: RaftShardRollbackParameters,
  ): Future[Seq[SnapshotSyncManagerRollbackSetup]] = {
    val snapshotSyncManagerIds = for {
      sourceMemberIndex      <- parameters.allMemberIndices
      destinationMemberIndex <- parameters.allMemberIndices if destinationMemberIndex != sourceMemberIndex
    } yield SnapshotSyncManagerId(parameters.typeName, parameters.shardId, sourceMemberIndex, destinationMemberIndex)
    Source(snapshotSyncManagerIds)
      .mapAsyncUnordered(settings.readParallelism) { snapshotSyncManagerId =>
        val toTimestamp = toTimestampFor(parameters, snapshotSyncManagerId.destinationMemberIndex)
        raftPersistence.sequenceNrSearchStrategy
          .findUpperBound(snapshotSyncManagerId.persistenceId, toTimestamp)
          .map { sequenceNr =>
            log.info(
              "Calculated SnapshotSyncManager rollback setup: persistence_id=[{}], to_sequence_nr=[{}]",
              snapshotSyncManagerId.persistenceId,
              sequenceNr.fold(0L)(_.value),
            )
            SnapshotSyncManagerRollbackSetup(snapshotSyncManagerId, sequenceNr)
          }
      }
      .runWith(Sink.seq)
  }

  /** Calculates and returns a CommitLogStore's rollback setup for the given Raft shard
    *
    * @see [[prepareRollback]]
    */
  def prepareCommitLogStoreActorRollback(
      parameters: RaftShardRollbackParameters,
      raftActorRollbackSetups: Seq[RaftActorRollbackSetup],
  ): Future[CommitLogStoreActorRollbackSetup] = {
    val leaderRaftActorId =
      RaftActorId(parameters.typeName, parameters.shardId, parameters.leaderMemberIndex)
    val leaderRaftActorRollbackSetup =
      raftActorRollbackSetups.find(_.id == leaderRaftActorId).getOrElse {
        throw new IllegalArgumentException(
          s"raftActorRollbackSetups [$raftActorRollbackSetups] should contain the setup " +
          s"whose id is equal to leader id [$leaderRaftActorId]",
        )
      }
    val commitLogStoreActorId =
      CommitLogStoreActorId(parameters.typeName, parameters.shardId)
    leaderRaftActorRollbackSetup.to match {
      case Some(leadersRollbackTo) =>
        raftPersistence.raftShardPersistenceQueries
          .findLastTruncatedLogEntryIndex(
            leaderRaftActorRollbackSetup.id,
            leadersRollbackTo,
          ).map { logEntryIndexOption =>
            // LogEntryIndex is equal to SequenceNr in CommitLogStoreActor
            val sequenceNrOption = logEntryIndexOption.map { logEntryIndex =>
              SequenceNr(logEntryIndex.underlying)
            }
            log.info(
              "Calculated CommitLogStoreActor rollback setup: persistence_id=[{}], to_sequence_nr=[{}]",
              commitLogStoreActorId.persistenceId,
              sequenceNrOption.fold(0L)(_.value),
            )
            CommitLogStoreActorRollbackSetup(commitLogStoreActorId, sequenceNrOption)
          }
      case None =>
        log.info(
          "Calculated CommitLogStoreActor rollback setup: persistence_id=[{}], to_sequence_nr=[0]",
          commitLogStoreActorId.persistenceId,
        )
        Future.successful(CommitLogStoreActorRollbackSetup(commitLogStoreActorId, None))
    }
  }

  private def toTimestampFor(parameters: RaftShardRollbackParameters, memberIndex: MemberIndex): Instant = {
    if (memberIndex == parameters.leaderMemberIndex) {
      parameters.toTimestamp
    } else {
      parameters.toTimestamp.minusMillis(settings.clockOutOfSyncTolerance.toMillis)
    }
  }

  /** Rolls back the Raft shard to the specific time point
    *
    * Note that this method doesn't verify the given setup is valid.
    * [[prepareRollback]] can calculates the valid setup for this method.
    *
    * @see [[prepareRollback]]
    */
  def rollback(setup: RaftShardRollbackSetup): Future[Done] = {
    if (settings.dryRun) {
      log.info(
        "dry-run: roll back for the Raft shard ([{}] raft persistence setups, [{}] raft event sourced persistence setups)",
        setup.raftPersistenceRollbackSetups.size,
        setup.raftEventSourcedPersistenceRollbackSetups.size,
      )
      // NOTE: continue to run the following, which logs info messages describing what action will be executed.
    }
    assert(
      raftPersistence.persistentActorRollback.isDryRun == settings.dryRun,
      "The underlying rollback for raft-persistence " +
      s"should have dry-run mode [${raftPersistence.persistentActorRollback.isDryRun}] " +
      s"the same as this dry-run mode [${settings.dryRun}]",
    )
    assert(
      raftEventSourcedPersistence.persistentActorRollback.isDryRun == settings.dryRun,
      "The underlying rollback for raft-eventsourced-persistence " +
      s"should have dry-run mode [${raftEventSourcedPersistence.persistentActorRollback.isDryRun}] " +
      s"the same as this dry-run mode [${settings.dryRun}]",
    )
    Future
      .sequence(
        Seq(
          rollbackAll(
            "raft-persistence",
            raftPersistence.persistentActorRollback,
            setup.raftPersistenceRollbackSetups,
          ),
          rollbackAll(
            "raft-eventsourced-persistence",
            raftEventSourcedPersistence.persistentActorRollback,
            setup.raftEventSourcedPersistenceRollbackSetups,
          ),
        ),
      ).map(_ => Done)
  }

  private def rollbackAll(
      jobName: String,
      rollback: PersistentActorRollback,
      setups: Seq[RollbackSetup],
  ): Future[Done] = {
    // NOTE: continue to run the following regardless of dry-run mode,
    //       which logs info messages describing what action will be executed.
    assert(rollback.isDryRun == settings.dryRun, s"[$rollback] for [$jobName] should have the same dry-run mode")
    val size = setups.size
    log.info("Started [{}] rollback for [{}]: dry-run=[{}]", size, jobName, settings.dryRun)
    val allRollbackJob = Source(setups).zipWithIndex
      .mapAsyncUnordered(settings.writeParallelism) {
        case (setup, index) =>
          val rollbackJob = setup.toSequenceNr match {
            case 0 =>
              rollback.deleteAll(setup.persistenceId)
            case toSequenceNr =>
              assert(toSequenceNr >= 0, s"toSequenceNr [$toSequenceNr] should be greater than 0")
              rollback.rollbackTo(setup.persistenceId, SequenceNr(toSequenceNr))
          }
          rollbackJob.foreach { _ =>
            if ((index + 1) % settings.logProgressEvery == 0) {
              log.info(
                "rollback progress [{}] of [{}] for [{}]: dry-run=[{}]",
                index + 1,
                size,
                jobName,
                settings.dryRun,
              )
            }
          }
          rollbackJob
      }
      .runWith(Sink.ignore)
    allRollbackJob
      .foreach { _ =>
        log.info("Completed [{}] rollback for [{}]: dry-run=[{}]", size, jobName, settings.dryRun)
      }
    allRollbackJob
  }

}
