package lerna.akka.entityreplication.rollback

/** Persistence operations for persistence plugin `lerna.akka.entityreplication.raft.eventsourced.persistence` */
private class RaftEventSourcedPersistence(
    val persistentActorRollback: PersistentActorRollback,
)
