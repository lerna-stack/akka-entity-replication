package lerna.akka.entityreplication.raft

import java.util.concurrent.TimeUnit.NANOSECONDS

import com.typesafe.config.Config

import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.Random

object RaftSettings {
  def apply(root: Config) = new RaftSettings(root)
}

class RaftSettings(root: Config) {

  val config: Config = root.getConfig("lerna.akka.entityreplication.raft")

  val electionTimeout: FiniteDuration = config.getDuration("election-timeout").toScala

  def randomizedElectionTimeout(): FiniteDuration = randomized(electionTimeout)

  val heartbeatInterval: FiniteDuration = config.getDuration("heartbeat-interval").toScala

  private[this] val randomizedMinFactor = 0.75

  private[this] val randomizedMaxFactor = 0.75

  /**
    * 75% - 150% of duration
    */
  def randomized(duration: FiniteDuration): FiniteDuration = {
    val randomizedDuration = duration * (randomizedMinFactor + randomizedMaxFactor * Random.nextDouble())
    FiniteDuration(randomizedDuration.toNanos, NANOSECONDS)
  }

  val electionTimeoutMin: Duration = electionTimeout * randomizedMinFactor

  // heartbeatInterval < electionTimeout < MTBF が満たせないと可用性が損なわれるため
  require(
    electionTimeoutMin > heartbeatInterval,
    s"electionTimeout (${electionTimeout.toMillis} ms) * $randomizedMinFactor must be larger than heartbeatInterval (${heartbeatInterval.toMillis} ms)",
  )

  val multiRaftRoles: Set[String] = config.getStringList("multi-raft-roles").asScala.toSet

  require(
    multiRaftRoles.size >= 3,
    s"multi-raft-roles should have size 3 or more for availability",
  )

  val replicationFactor: Int = multiRaftRoles.size

  val quorumSize: Int = (replicationFactor / 2) + 1

  val maxAppendEntriesSize: Int = config.getInt("max-append-entries-size")

  val maxAppendEntriesBatchSize: Int = config.getInt("max-append-entries-batch-size")

  val compactionSnapshotCacheTimeToLive: FiniteDuration =
    config.getDuration("compaction.snapshot-cache-time-to-live").toScala

  val compactionLogSizeThreshold: Int = config.getInt("compaction.log-size-threshold")

  require(
    0 < compactionLogSizeThreshold,
    s"log-size-threshold ($compactionLogSizeThreshold) should be larger than 0",
  )

  val compactionPreserveLogSize: Int = config.getInt("compaction.preserve-log-size")

  require(
    0 < compactionPreserveLogSize,
    s"preserve-log-size ($compactionPreserveLogSize) should be larger than 0",
  )

  val compactionLogSizeCheckInterval: FiniteDuration = config.getDuration("compaction.log-size-check-interval").toScala

  def randomizedCompactionLogSizeCheckInterval(): FiniteDuration = randomized(compactionLogSizeCheckInterval)

  val snapshotSyncCopyingParallelism: Int = config.getInt("snapshot-sync.snapshot-copying-parallelism")

  val snapshotSyncPersistenceOperationTimeout: FiniteDuration =
    config.getDuration("snapshot-sync.persistence-operation-timeout").toScala

  val clusterShardingConfig: Config = config.getConfig("sharding")

  val journalPluginId: String = config.getString("persistence.journal.plugin")

  val snapshotStorePluginId: String = config.getString("persistence.snapshot-store.plugin")

  val queryPluginId: String = config.getString("persistence.query.plugin")
}
