package lerna.akka.entityreplication.raft

import com.typesafe.config.{ Config, ConfigFactory }

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }
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
    disabledShards: Set[String],
    maxAppendEntriesSize: Int,
    maxAppendEntriesBatchSize: Int,
    compactionSnapshotCacheTimeToLive: FiniteDuration,
    compactionLogSizeThreshold: Int,
    compactionPreserveLogSize: Int,
    compactionLogSizeCheckInterval: FiniteDuration,
    snapshotSyncCopyingParallelism: Int,
    snapshotSyncPersistenceOperationTimeout: FiniteDuration,
    snapshotSyncMaxSnapshotBatchSize: Int,
    clusterShardingConfig: Config,
    raftActorAutoStartFrequency: FiniteDuration,
    raftActorAutoStartNumberOfActors: Int,
    raftActorAutoStartRetryInterval: FiniteDuration,
    journalPluginId: String,
    snapshotStorePluginId: String,
    queryPluginId: String,
    eventSourcedCommittedLogEntriesCheckInterval: FiniteDuration,
    eventSourcedMaxAppendCommittedEntriesSize: Int,
    eventSourcedMaxAppendCommittedEntriesBatchSize: Int,
    eventSourcedJournalPluginId: String,
    eventSourcedSnapshotStorePluginId: String,
    eventSourcedSnapshotEvery: Int,
) extends RaftSettings {

  override private[raft] def randomizedElectionTimeout(): FiniteDuration =
    RaftSettingsImpl.randomized(electionTimeout)

  override private[raft] def randomizedCompactionLogSizeCheckInterval(): FiniteDuration =
    RaftSettingsImpl.randomized(compactionLogSizeCheckInterval)

  override def journalPluginAdditionalConfig: Config =
    ConfigFactory.parseMap {
      Map(
        journalPluginId -> config.getObject("persistence.journal-plugin-additional"),
      ).asJava
    }

  override private[entityreplication] def withDisabledShards(disabledShards: Set[String]): RaftSettings =
    copy(disabledShards = disabledShards)

  override private[entityreplication] def withJournalPluginId(pluginId: String): RaftSettings =
    copy(journalPluginId = pluginId)

  override private[entityreplication] def withSnapshotPluginId(pluginId: String): RaftSettings =
    copy(snapshotStorePluginId = pluginId)

  override private[entityreplication] def withQueryPluginId(pluginId: String): RaftSettings =
    copy(queryPluginId = pluginId)

  override private[entityreplication] def withEventSourcedJournalPluginId(pluginId: String): RaftSettings =
    copy(eventSourcedJournalPluginId = pluginId)

  override private[entityreplication] def withEventSourcedSnapshotStorePluginId(pluginId: String): RaftSettings =
    copy(eventSourcedSnapshotStorePluginId = pluginId)

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

    val disabledShards: Set[String] = Set.empty

    val maxAppendEntriesSize: Int = config.getInt("max-append-entries-size")
    require(
      maxAppendEntriesSize > 0,
      s"max-append-entries-size ($maxAppendEntriesSize) should be greater than 0",
    )

    val maxAppendEntriesBatchSize: Int = config.getInt("max-append-entries-batch-size")
    require(
      maxAppendEntriesBatchSize > 0,
      s"max-append-entries-batch-size ($maxAppendEntriesBatchSize) should be greater than 0",
    )

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
    require(
      compactionPreserveLogSize < compactionLogSizeThreshold,
      s"preserve-log-size ($compactionPreserveLogSize) should be less than log-size-threshold ($compactionLogSizeThreshold).",
    )

    val compactionLogSizeCheckInterval: FiniteDuration =
      config.getDuration("compaction.log-size-check-interval").toScala

    val snapshotSyncCopyingParallelism: Int = config.getInt("snapshot-sync.snapshot-copying-parallelism")

    val snapshotSyncPersistenceOperationTimeout: FiniteDuration =
      config.getDuration("snapshot-sync.persistence-operation-timeout").toScala

    val snapshotSyncMaxSnapshotBatchSize: Int =
      config.getInt("snapshot-sync.max-snapshot-batch-size")
    require(
      snapshotSyncMaxSnapshotBatchSize > 0,
      s"snapshot-sync.max-snapshot-batch-size (${snapshotSyncMaxSnapshotBatchSize}) should be larger than 0",
    )

    val clusterShardingConfig: Config = config.getConfig("sharding")

    val raftActorAutoStartFrequency: FiniteDuration =
      config.getDuration("raft-actor-auto-start.frequency").toScala
    require(
      raftActorAutoStartFrequency > 0.milli,
      s"raft-actor-auto-start.frequency ($raftActorAutoStartFrequency) should be greater than 0 milli.",
    )

    val raftActorAutoStartNumberOfActors: Int =
      config.getInt("raft-actor-auto-start.number-of-actors")
    require(
      raftActorAutoStartNumberOfActors > 0,
      s"raft-actor-auto-start.number-of-actors ($raftActorAutoStartNumberOfActors) should be greater than 0.",
    )

    val raftActorAutoStartRetryInterval: FiniteDuration =
      config.getDuration("raft-actor-auto-start.retry-interval").toScala
    require(
      raftActorAutoStartRetryInterval > 0.milli,
      s"raft-actor-auto-start.retry-interval ($raftActorAutoStartRetryInterval) should be greater than 0 milli.",
    )

    val journalPluginId: String = config.getString("persistence.journal.plugin")

    val snapshotStorePluginId: String = config.getString("persistence.snapshot-store.plugin")

    val queryPluginId: String = config.getString("persistence.query.plugin")

    val eventSourcedCommittedLogEntriesCheckInterval: FiniteDuration =
      config.getDuration("eventsourced.committed-log-entries-check-interval").toScala
    require(
      eventSourcedCommittedLogEntriesCheckInterval > 0.milli,
      s"eventsourced.committed-log-entries-check-interval (${eventSourcedCommittedLogEntriesCheckInterval.toMillis}ms) should be greater than 0 milli.",
    )

    val eventSourcedMaxAppendCommittedEntriesSize: Int =
      config.getInt("eventsourced.max-append-committed-entries-size")
    require(
      eventSourcedMaxAppendCommittedEntriesSize > 0,
      s"eventsourced.max-append-committed-entries-size ($eventSourcedMaxAppendCommittedEntriesSize) should be greater than 0.",
    )

    val eventSourcedMaxAppendCommittedEntriesBatchSize: Int =
      config.getInt("eventsourced.max-append-committed-entries-batch-size")
    require(
      eventSourcedMaxAppendCommittedEntriesBatchSize > 0,
      s"eventsourced.max-append-committed-entries-batch-size ($eventSourcedMaxAppendCommittedEntriesBatchSize) should be greater than 0.",
    )

    val eventSourcedJournalPluginId: String = config.getString("eventsourced.persistence.journal.plugin")

    val eventSourcedSnapshotStorePluginId: String = config.getString("eventsourced.persistence.snapshot-store.plugin")

    val eventSourcedSnapshotEvery: Int = config.getInt("eventsourced.persistence.snapshot-every")
    require(
      eventSourcedSnapshotEvery > 0,
      s"snapshot-every ($eventSourcedSnapshotEvery) should be greater than 0.",
    )

    RaftSettingsImpl(
      config = config,
      electionTimeout = electionTimeout,
      heartbeatInterval = heartbeatInterval,
      electionTimeoutMin = electionTimeoutMin,
      multiRaftRoles = multiRaftRoles,
      replicationFactor = replicationFactor,
      quorumSize = quorumSize,
      numberOfShards = numberOfShards,
      disabledShards = disabledShards,
      maxAppendEntriesSize = maxAppendEntriesSize,
      maxAppendEntriesBatchSize = maxAppendEntriesBatchSize,
      compactionSnapshotCacheTimeToLive = compactionSnapshotCacheTimeToLive,
      compactionLogSizeThreshold = compactionLogSizeThreshold,
      compactionPreserveLogSize = compactionPreserveLogSize,
      compactionLogSizeCheckInterval = compactionLogSizeCheckInterval,
      snapshotSyncCopyingParallelism = snapshotSyncCopyingParallelism,
      snapshotSyncPersistenceOperationTimeout = snapshotSyncPersistenceOperationTimeout,
      snapshotSyncMaxSnapshotBatchSize = snapshotSyncMaxSnapshotBatchSize,
      clusterShardingConfig = clusterShardingConfig,
      raftActorAutoStartFrequency = raftActorAutoStartFrequency,
      raftActorAutoStartNumberOfActors = raftActorAutoStartNumberOfActors,
      raftActorAutoStartRetryInterval = raftActorAutoStartRetryInterval,
      journalPluginId = journalPluginId,
      snapshotStorePluginId = snapshotStorePluginId,
      queryPluginId = queryPluginId,
      eventSourcedCommittedLogEntriesCheckInterval = eventSourcedCommittedLogEntriesCheckInterval,
      eventSourcedMaxAppendCommittedEntriesSize,
      eventSourcedMaxAppendCommittedEntriesBatchSize,
      eventSourcedJournalPluginId = eventSourcedJournalPluginId,
      eventSourcedSnapshotStorePluginId = eventSourcedSnapshotStorePluginId,
      eventSourcedSnapshotEvery = eventSourcedSnapshotEvery,
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
