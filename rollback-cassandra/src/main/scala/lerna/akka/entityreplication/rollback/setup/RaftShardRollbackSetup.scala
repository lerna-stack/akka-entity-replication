package lerna.akka.entityreplication.rollback.setup

private[rollback] object RaftShardRollbackSetup {

  def apply(
      raftActorRollbackSetups: Seq[RaftActorRollbackSetup],
      snapshotStoreRollbackSetups: Seq[SnapshotStoreRollbackSetup],
      snapshotSyncManagerRollbackSetups: Seq[SnapshotSyncManagerRollbackSetup],
      commitLogStoreActorRollbackSetup: CommitLogStoreActorRollbackSetup,
  ): RaftShardRollbackSetup = {
    val raftPersistenceRollbackSetups: Seq[RollbackSetup] = {
      raftActorRollbackSetups.map(setup => RollbackSetup(setup.id.persistenceId, setup.to.fold(0L)(_.value))) ++
      snapshotStoreRollbackSetups.map(setup => RollbackSetup(setup.id.persistenceId, setup.to.fold(0L)(_.value))) ++
      snapshotSyncManagerRollbackSetups.map(setup => RollbackSetup(setup.id.persistenceId, setup.to.fold(0L)(_.value)))
    }
    val raftEventSourcedPersistenceRollbackSetups: Seq[RollbackSetup] = Seq(
      RollbackSetup(
        commitLogStoreActorRollbackSetup.id.persistenceId,
        commitLogStoreActorRollbackSetup.to.fold(0L)(_.value),
      ),
    )
    new RaftShardRollbackSetup(raftPersistenceRollbackSetups, raftEventSourcedPersistenceRollbackSetups)
  }

}

/** Rollback setup for one Raft shard */
final class RaftShardRollbackSetup private[rollback] (
    val raftPersistenceRollbackSetups: Seq[RollbackSetup],
    val raftEventSourcedPersistenceRollbackSetups: Seq[RollbackSetup],
)
