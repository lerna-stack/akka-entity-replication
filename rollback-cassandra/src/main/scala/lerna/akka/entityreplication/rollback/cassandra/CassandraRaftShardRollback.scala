package lerna.akka.entityreplication.rollback.cassandra

import akka.Done
import akka.actor.ClassicActorSystemProvider
import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.rollback._
import lerna.akka.entityreplication.rollback.setup.RaftShardRollbackSetup

import java.time.Instant
import scala.concurrent.Future

object CassandraRaftShardRollback {

  /** Creates a [[CassandraRaftShardRollback]] from the given system */
  def apply(system: ClassicActorSystemProvider): CassandraRaftShardRollback = {
    val settings = CassandraRaftShardRollbackSettings(system)
    CassandraRaftShardRollback(system, settings)
  }

  /** Creates a [[CassandraRaftShardRollback]] from the given system and settings */
  def apply(
      system: ClassicActorSystemProvider,
      settings: CassandraRaftShardRollbackSettings,
  ): CassandraRaftShardRollback = {
    new CassandraRaftShardRollback(system, settings)
  }

}

/** Provides rolling back for the Raft shard to the specific timestamp */
final class CassandraRaftShardRollback private (
    systemProvider: ClassicActorSystemProvider,
    val settings: CassandraRaftShardRollbackSettings,
) {

  private[cassandra] val underlyingRollback: RaftShardRollback = {
    val raftPersistence = {
      val rollback =
        new CassandraPersistentActorRollback(systemProvider, settings.raftPersistenceRollbackSettings)
      val queries =
        new RaftShardPersistenceQueries(rollback.persistenceQueries)
      val sequenceNrSearchStrategy =
        new LinearSequenceNrSearchStrategy(systemProvider, rollback.persistenceQueries)
      new RaftPersistence(rollback, queries, sequenceNrSearchStrategy)
    }
    val raftEventSourcedPersistence = {
      val rollback =
        new CassandraPersistentActorRollback(systemProvider, settings.raftEventSourcedPersistenceRollbackSettings)
      new RaftEventSourcedPersistence(rollback)
    }
    new RaftShardRollback(
      systemProvider,
      settings.rollbackSettings,
      raftPersistence,
      raftEventSourcedPersistence,
    )
  }

  /** Calculates and returns a rollback setup for the given Raft shard
    *
    * Note that this method doesn't execute actual rollback. [[rollback]] takes the rollback setup returned by this
    * method and then executes the actual rollback. The setup can be used to review how rollback is executed.
    *
    * @see [[rollback]]
    */
  def prepareRollback(
      typeName: String,
      shardId: String,
      multiRaftRoles: Set[String],
      leaderRaftRole: String,
      toTimestamp: Instant,
  ): Future[RaftShardRollbackSetup] = {
    val allMemberIndices =
      multiRaftRoles.map(MemberIndex(_))
    val leaderMemberIndex =
      MemberIndex(leaderRaftRole)
    val parameters =
      RaftShardRollbackParameters(
        TypeName.from(typeName),
        NormalizedShardId.from(shardId),
        allMemberIndices,
        leaderMemberIndex,
        toTimestamp,
      )
    underlyingRollback.prepareRollback(parameters)
  }

  /** Rolls back the Raft shard to the specific point
    *
    * Note that this method doesn't verify the given setup is valid.
    * [[prepareRollback]] can calculates the valid setup for this method.
    *
    * @see [[prepareRollback]]
    */
  def rollback(setup: RaftShardRollbackSetup): Future[Done] = {
    underlyingRollback.rollback(setup)
  }

}
