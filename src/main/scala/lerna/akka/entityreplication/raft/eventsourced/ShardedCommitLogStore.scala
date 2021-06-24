package lerna.akka.entityreplication.raft.eventsourced

import akka.actor.{ ActorSystem, Scheduler }
import akka.pattern.ask
import akka.util.Timeout
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.model.LogEntryIndex

import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

private[entityreplication] class ShardedCommitLogStore(
    typeName: TypeName,
    system: ActorSystem,
    settings: ClusterReplicationSettings,
) extends CommitLogStore {
  import system.dispatcher
  private implicit val scheduler: Scheduler = system.scheduler

  private val config =
    system.settings.config.getConfig("lerna.akka.entityreplication.raft.eventsourced.commit-log-store")
  private val retryAttempts                  = config.getInt("retry.attempts")
  private val retryDelay: FiniteDuration     = config.getDuration("retry.delay").toScala
  private implicit val retryTimeout: Timeout = Timeout(retryDelay)

  private val shardRegion = CommitLogStoreActor.startClusterSharding(typeName, system, settings)

  override private[raft] def save(
      shardId: NormalizedShardId,
      index: LogEntryIndex,
      committedEvent: Any,
  ): Unit = {
    akka.pattern.retry(
      () => shardRegion ? Save(shardId, index, committedEvent),
      attempts = retryAttempts,
      delay = retryDelay,
    )
  }
}
