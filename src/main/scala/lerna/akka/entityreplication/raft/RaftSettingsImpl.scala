package lerna.akka.entityreplication.raft

import com.typesafe.config.{ Config, ConfigFactory }

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.util.Random

private[entityreplication] final case class RaftSettingsImpl(
    config: Config,
    electionTimeout: FiniteDuration,
    heartbeatInterval: FiniteDuration,
    electionTimeoutMin: Duration,
    multiRaftRoles: Set[String],
    replicationFactor: Int,
    quorumSize: Int,
    numberOfShards: Int,
    maxAppendEntriesSize: Int,
    maxAppendEntriesBatchSize: Int,
    compactionSnapshotCacheTimeToLive: FiniteDuration,
    compactionLogSizeThreshold: Int,
    compactionPreserveLogSize: Int,
    compactionLogSizeCheckInterval: FiniteDuration,
    snapshotSyncCopyingParallelism: Int,
    snapshotSyncPersistenceOperationTimeout: FiniteDuration,
    clusterShardingConfig: Config,
    journalPluginId: String,
    journalPluginAdditionalConfig: Config,
    snapshotStorePluginId: String,
    queryPluginId: String,
    eventSourcedJournalPluginId: String,
) extends RaftSettings {

  override private[raft] def randomizedElectionTimeout(): FiniteDuration =
    RaftSettingsImpl.randomized(electionTimeout)

  override private[raft] def randomizedCompactionLogSizeCheckInterval(): FiniteDuration =
    RaftSettingsImpl.randomized(compactionLogSizeCheckInterval)

}

private[entityreplication] object RaftSettingsImpl {

  def apply(root: Config): RaftSettingsImpl = {

    val config: Config = root.getConfig("lerna.akka.entityreplication.raft")

    val electionTimeout: FiniteDuration = config.getDuration("election-timeout").toScala

    val heartbeatInterval: FiniteDuration = config.getDuration("heartbeat-interval").toScala

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

    val numberOfShards: Int = config.getInt("number-of-shards")

    require(
      numberOfShards > 0,
      s"number-of-shards ($numberOfShards) should be larger than 0",
    )

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

    val compactionLogSizeCheckInterval: FiniteDuration =
      config.getDuration("compaction.log-size-check-interval").toScala

    val snapshotSyncCopyingParallelism: Int = config.getInt("snapshot-sync.snapshot-copying-parallelism")

    val snapshotSyncPersistenceOperationTimeout: FiniteDuration =
      config.getDuration("snapshot-sync.persistence-operation-timeout").toScala

    val clusterShardingConfig: Config = config.getConfig("sharding")

    val journalPluginId: String = config.getString("persistence.journal.plugin")

    val journalPluginAdditionalConfig: Config =
      ConfigFactory.parseMap {
        Map(
          journalPluginId -> config.getObject("persistence.journal-plugin-additional"),
        ).asJava
      }

    val snapshotStorePluginId: String = config.getString("persistence.snapshot-store.plugin")

    val queryPluginId: String = config.getString("persistence.query.plugin")

    val eventSourcedJournalPluginId: String = config.getString("eventsourced.persistence.journal.plugin")

    RaftSettingsImpl(
      config = config,
      electionTimeout = electionTimeout,
      heartbeatInterval = heartbeatInterval,
      electionTimeoutMin = electionTimeoutMin,
      multiRaftRoles = multiRaftRoles,
      replicationFactor = replicationFactor,
      quorumSize = quorumSize,
      numberOfShards = numberOfShards,
      maxAppendEntriesSize = maxAppendEntriesSize,
      maxAppendEntriesBatchSize = maxAppendEntriesBatchSize,
      compactionSnapshotCacheTimeToLive = compactionSnapshotCacheTimeToLive,
      compactionLogSizeThreshold = compactionLogSizeThreshold,
      compactionPreserveLogSize = compactionPreserveLogSize,
      compactionLogSizeCheckInterval = compactionLogSizeCheckInterval,
      snapshotSyncCopyingParallelism = snapshotSyncCopyingParallelism,
      snapshotSyncPersistenceOperationTimeout = snapshotSyncPersistenceOperationTimeout,
      clusterShardingConfig = clusterShardingConfig,
      journalPluginId = journalPluginId,
      journalPluginAdditionalConfig = journalPluginAdditionalConfig,
      snapshotStorePluginId = snapshotStorePluginId,
      queryPluginId = queryPluginId,
      eventSourcedJournalPluginId = eventSourcedJournalPluginId,
    )
  }

  /**
    * 75% - 150% of duration
    */
  private def randomized(duration: FiniteDuration): FiniteDuration = {
    val randomizedDuration = duration * (randomizedMinFactor + randomizedMaxFactor * Random.nextDouble())
    FiniteDuration(randomizedDuration.toNanos, NANOSECONDS)
  }

  private val randomizedMinFactor = 0.75

  private val randomizedMaxFactor = 0.75
}
