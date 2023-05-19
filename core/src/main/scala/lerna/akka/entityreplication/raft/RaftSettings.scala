package lerna.akka.entityreplication.raft

import com.typesafe.config.Config

import scala.concurrent.duration.{ Duration, FiniteDuration }

private[entityreplication] object RaftSettings {
  def apply(root: Config): RaftSettings = RaftSettingsImpl(root)
}

trait RaftSettings {

  def config: Config

  def electionTimeout: FiniteDuration

  def stickyLeaders: Map[String, String]

  private[raft] def randomizedElectionTimeout(): FiniteDuration

  def heartbeatInterval: FiniteDuration

  def electionTimeoutMin: Duration

  def multiRaftRoles: Set[String]

  def replicationFactor: Int

  def quorumSize: Int

  def numberOfShards: Int

  /** Shard IDs of Raft actors to disable
    *
    * The default value is `Set.empty`, which means no RaftActors are disabled. This value is not loaded from the config
    * to preventing stopping RaftActors across all type names. It only can be changed via [[withDisabledShards]].
    */
  def disabledShards: Set[String]

  def maxAppendEntriesSize: Int

  def maxAppendEntriesBatchSize: Int

  /** Returns `true` if a Raft actor (`RaftActor`) deletes old events when it successfully saves a
    * snapshot, `false` otherwise.
    */
  def deleteOldEvents: Boolean

  /** Returns `true` if a Raft actor (`RaftActor`) deletes old snapshots when it successfully saves
    * a snapshot, `false` otherwise.
    */
  def deleteOldSnapshots: Boolean

  /** Returns the relative sequence number for determining old events and snapshots to be deleted.
    *
    * For more details, see the setting `lerna.akka.entityreplication.raft.delete-before-relative-sequence-nr`.
    */
  def deleteBeforeRelativeSequenceNr: Long

  def compactionSnapshotCacheTimeToLive: FiniteDuration

  def compactionLogSizeThreshold: Int

  def compactionPreserveLogSize: Int

  def compactionLogSizeCheckInterval: FiniteDuration

  private[raft] def randomizedCompactionLogSizeCheckInterval(): FiniteDuration

  def snapshotSyncCopyingParallelism: Int

  def snapshotSyncPersistenceOperationTimeout: FiniteDuration

  def snapshotSyncMaxSnapshotBatchSize: Int

  /** Returns `true` if a `SnapshotSyncManager` deletes old events when it successfully saves a
    * snapshot, `false` otherwise.
    */
  def snapshotSyncDeleteOldEvents: Boolean

  /** Returns `true` if a `SnapshotSyncManager` deletes old snapshots when it successfully saves
    * a snapshot, `false` otherwise.
    */
  def snapshotSyncDeleteOldSnapshots: Boolean

  /** Returns the relative sequence number for determining old events and snapshots to be deleted.
    *
    * For more details, see the setting `lerna.akka.entityreplication.raft.snapshot-sync.delete-before-relative-sequence-nr`.
    */
  def snapshotSyncDeleteBeforeRelativeSequenceNr: Long

  def snapshotStoreSnapshotEvery: Int

  /** Returns `true` if an EntitySnapshotStore (`raft.snapshot.SnapshotStore`) deletes old events when it successfully
    * saves a snapshot, `false` otherwise.
    */
  def snapshotStoreDeleteOldEvents: Boolean

  /** Returns `true` if an EntitySnapshotStore (`raft.snapshot.SnapshotStore`) deletes old snapshots when it successfully
    * saves a snapshot, `false` otherwise.
    */
  def snapshotStoreDeleteOldSnapshots: Boolean

  /** Returns the relative sequence number for determining old events and snapshots to be deleted.
    *
    * For more details, see the setting `lerna.akka.entityreplication.raft.entity-snapshot-store.delete-before-relative-sequence-nr`.
    */
  def snapshotStoreDeleteBeforeRelativeSequenceNr: Long

  def clusterShardingConfig: Config

  def raftActorAutoStartFrequency: FiniteDuration

  def raftActorAutoStartNumberOfActors: Int

  def raftActorAutoStartRetryInterval: FiniteDuration

  def journalPluginId: String

  def journalPluginAdditionalConfig: Config

  def snapshotStorePluginId: String

  def queryPluginId: String

  def eventSourcedCommittedLogEntriesCheckInterval: FiniteDuration

  def eventSourcedMaxAppendCommittedEntriesSize: Int

  def eventSourcedMaxAppendCommittedEntriesBatchSize: Int

  def eventSourcedJournalPluginId: String

  def eventSourcedSnapshotStorePluginId: String

  def eventSourcedSnapshotEvery: Int

  /** Returns `true` if an event-sourcing store (`CommitLogStoreActor`) deletes old events when it successfully saves a
    * snapshot, `false` otherwise.
    */
  def eventSourcedDeleteOldEvents: Boolean

  /** Returns `true` if an event-sourcing store (`CommitLogStoreActor`) deletes old snapshots when it successfully saves
    * a snapshot, `false` otherwise.
    */
  def eventSourcedDeleteOldSnapshots: Boolean

  /** Returns the relative sequence number for determining old events and snapshots to be deleted.
    *
    * For more details, see the setting `lerna.akka.entityreplication.raft.eventsourced.persistence.delete-before-relative-sequence-nr`.
    */
  def eventSourcedDeleteBeforeRelativeSequenceNr: Long

  private[entityreplication] def withDisabledShards(disabledShards: Set[String]): RaftSettings

  private[entityreplication] def withStickyLeaders(stickyLeaders: Map[String, String]): RaftSettings

  private[entityreplication] def withJournalPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withSnapshotPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withQueryPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withEventSourcedJournalPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withEventSourcedSnapshotStorePluginId(pluginId: String): RaftSettings

}
