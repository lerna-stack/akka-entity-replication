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
    stickyLeaders: Map[String, String],
    heartbeatInterval: FiniteDuration,
    electionTimeoutMin: Duration,
    multiRaftRoles: Set[String],
    replicationFactor: Int,
    quorumSize: Int,
    numberOfShards: Int,
    disabledShards: Set[String],
    maxAppendEntriesSize: Int,
    maxAppendEntriesBatchSize: Int,
    deleteOldEvents: Boolean,
    deleteOldSnapshots: Boolean,
    deleteBeforeRelativeSequenceNr: Long,
    compactionSnapshotCacheTimeToLive: FiniteDuration,
    compactionLogSizeThreshold: Int,
    compactionPreserveLogSize: Int,
    compactionLogSizeCheckInterval: FiniteDuration,
    snapshotSyncCopyingParallelism: Int,
    snapshotSyncPersistenceOperationTimeout: FiniteDuration,
    snapshotSyncMaxSnapshotBatchSize: Int,
    snapshotSyncDeleteOldEvents: Boolean,
    snapshotSyncDeleteOldSnapshots: Boolean,
    snapshotSyncDeleteBeforeRelativeSequenceNr: Long,
    snapshotStoreSnapshotEvery: Int,
    snapshotStoreDeleteOldEvents: Boolean,
    snapshotStoreDeleteOldSnapshots: Boolean,
    snapshotStoreDeleteBeforeRelativeSequenceNr: Long,
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
    eventSourcedDeleteOldEvents: Boolean,
    eventSourcedDeleteOldSnapshots: Boolean,
    eventSourcedDeleteBeforeRelativeSequenceNr: Long,
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

  override private[entityreplication] def withStickyLeaders(stickyLeaders: Map[String, String]) =
    copy(stickyLeaders = stickyLeaders)

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

    val stickyLeaders: Map[String, String] = Map.empty

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

    val deleteOldEvents: Boolean =
      config.getBoolean("delete-old-events")
    val deleteOldSnapshots: Boolean =
      config.getBoolean("delete-old-snapshots")
    val deleteBeforeRelativeSequenceNr: Long =
      config.getLong("delete-before-relative-sequence-nr")
    require(
      deleteBeforeRelativeSequenceNr >= 0,
      s"delete-before-relative-sequence-nr ($deleteBeforeRelativeSequenceNr) should be greater than or equal to 0.",
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

    val snapshotSyncDeleteOldEvents: Boolean =
      config.getBoolean("snapshot-sync.delete-old-events")
    val snapshotSyncDeleteOldSnapshots: Boolean =
      config.getBoolean("snapshot-sync.delete-old-snapshots")
    val snapshotSyncDeleteBeforeRelativeSequenceNr: Long =
      config.getLong("snapshot-sync.delete-before-relative-sequence-nr")
    require(
      snapshotSyncDeleteBeforeRelativeSequenceNr >= 0,
      s"snapshot-sync.delete-before-relative-sequence-nr ($snapshotSyncDeleteBeforeRelativeSequenceNr) should be greater than or equal to 0.",
    )

    val snapshotStoreSnapshotEvery: Int = config.getInt("entity-snapshot-store.snapshot-every")
    require(
      snapshotStoreSnapshotEvery > 0,
      s"entity-snapshot-store.snapshot-every ($snapshotStoreSnapshotEvery) should be larger than 0",
    )

    val snapshotStoreDeleteOldEvents: Boolean =
      config.getBoolean("entity-snapshot-store.delete-old-events")
    val snapshotStoreDeleteOldSnapshots: Boolean =
      config.getBoolean("entity-snapshot-store.delete-old-snapshots")
    val snapshotStoreDeleteBeforeRelativeSequenceNr: Long =
      config.getLong("entity-snapshot-store.delete-before-relative-sequence-nr")
    require(
      snapshotStoreDeleteBeforeRelativeSequenceNr >= 0,
      s"entity-snapshot-store.delete-before-relative-sequence-nr ($snapshotStoreDeleteBeforeRelativeSequenceNr) should be greater than or equal to 0.",
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

    val eventSourcedDeleteOldEvents: Boolean =
      config.getBoolean("eventsourced.persistence.delete-old-events")
    val eventSourcedDeleteOldSnapshots: Boolean =
      config.getBoolean("eventsourced.persistence.delete-old-snapshots")
    val eventSourcedDeleteBeforeRelativeSequenceNr: Long =
      config.getLong("eventsourced.persistence.delete-before-relative-sequence-nr")
    require(
      eventSourcedDeleteBeforeRelativeSequenceNr >= 0,
      s"eventsourced.persistence.delete-before-relative-sequence-nr ($eventSourcedDeleteBeforeRelativeSequenceNr) should be greater than or equal to 0.",
    )

    RaftSettingsImpl(
      config = config,
      electionTimeout = electionTimeout,
      stickyLeaders = stickyLeaders,
      heartbeatInterval = heartbeatInterval,
      electionTimeoutMin = electionTimeoutMin,
      multiRaftRoles = multiRaftRoles,
      replicationFactor = replicationFactor,
      quorumSize = quorumSize,
      numberOfShards = numberOfShards,
      disabledShards = disabledShards,
      maxAppendEntriesSize = maxAppendEntriesSize,
      maxAppendEntriesBatchSize = maxAppendEntriesBatchSize,
      deleteOldEvents = deleteOldEvents,
      deleteOldSnapshots = deleteOldSnapshots,
      deleteBeforeRelativeSequenceNr = deleteBeforeRelativeSequenceNr,
      compactionSnapshotCacheTimeToLive = compactionSnapshotCacheTimeToLive,
      compactionLogSizeThreshold = compactionLogSizeThreshold,
      compactionPreserveLogSize = compactionPreserveLogSize,
      compactionLogSizeCheckInterval = compactionLogSizeCheckInterval,
      snapshotSyncCopyingParallelism = snapshotSyncCopyingParallelism,
      snapshotSyncPersistenceOperationTimeout = snapshotSyncPersistenceOperationTimeout,
      snapshotSyncMaxSnapshotBatchSize = snapshotSyncMaxSnapshotBatchSize,
      snapshotSyncDeleteOldEvents = snapshotSyncDeleteOldEvents,
      snapshotSyncDeleteOldSnapshots = snapshotSyncDeleteOldSnapshots,
      snapshotSyncDeleteBeforeRelativeSequenceNr = snapshotSyncDeleteBeforeRelativeSequenceNr,
      snapshotStoreSnapshotEvery = snapshotStoreSnapshotEvery,
      snapshotStoreDeleteOldEvents = snapshotStoreDeleteOldEvents,
      snapshotStoreDeleteOldSnapshots = snapshotStoreDeleteOldSnapshots,
      snapshotStoreDeleteBeforeRelativeSequenceNr = snapshotStoreDeleteBeforeRelativeSequenceNr,
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
      eventSourcedDeleteOldEvents = eventSourcedDeleteOldEvents,
      eventSourcedDeleteOldSnapshots = eventSourcedDeleteOldSnapshots,
      eventSourcedDeleteBeforeRelativeSequenceNr = eventSourcedDeleteBeforeRelativeSequenceNr,
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
