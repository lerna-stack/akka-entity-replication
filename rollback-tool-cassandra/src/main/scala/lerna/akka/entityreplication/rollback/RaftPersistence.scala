package lerna.akka.entityreplication.rollback

/** Persistence operations for persisntence plugin `lerna.akka.entityreplication.raft.persistence` */
private class RaftPersistence(
    val persistentActorRollback: PersistentActorRollback,
    val raftShardPersistenceQueries: RaftShardPersistenceQueries,
    val sequenceNrSearchStrategy: SequenceNrSearchStrategy,
    val requirementsVerifier: RollbackRequirementsVerifier,
)
