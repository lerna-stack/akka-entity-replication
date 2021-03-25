package lerna.akka.entityreplication.raft.eventsourced

import akka.actor.{ ActorSystem, Scheduler }
import akka.pattern.ask
import akka.util.Timeout
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStore.ReplicationId
import lerna.akka.entityreplication.raft.model.LogEntryIndex

import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

class ShardedCommitLogStore(typeName: String, system: ActorSystem) extends CommitLogStore {
  import system.dispatcher
  private implicit val scheduler: Scheduler = system.scheduler

  private val config =
    system.settings.config.getConfig("lerna.akka.entityreplication.raft.eventsourced.commit-log-store")
  private val retryAttempts                  = config.getInt("retry.attempts")
  private val retryDelay: FiniteDuration     = config.getDuration("retry.delay").toScala
  private implicit val retryTimeout: Timeout = Timeout(retryDelay)

  private val shardRegion = CommitLogStoreActor.startClusterSharding(typeName, system)

  override private[raft] def save(
      replicationId: ReplicationId,
      index: LogEntryIndex,
      committedEvent: Any,
  ): Unit = {
    akka.pattern.retry(
      () => shardRegion ? Save(replicationId, index, committedEvent),
      attempts = retryAttempts,
      delay = retryDelay,
    )
  }
}
