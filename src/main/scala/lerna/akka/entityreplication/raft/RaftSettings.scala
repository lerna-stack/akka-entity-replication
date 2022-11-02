package lerna.akka.entityreplication.raft

import com.typesafe.config.Config

import scala.concurrent.duration.{ Duration, FiniteDuration }

private[entityreplication] object RaftSettings {
  def apply(root: Config): RaftSettings = RaftSettingsImpl(root)
}

trait RaftSettings {

  def config: Config

  def electionTimeout: FiniteDuration

  private[raft] def randomizedElectionTimeout(): FiniteDuration

  def heartbeatInterval: FiniteDuration

  def electionTimeoutMin: Duration

  def multiRaftRoles: Set[String]

  def replicationFactor: Int

  def quorumSize: Int

  def numberOfShards: Int

  def disabledShards: Set[String]

  def maxAppendEntriesSize: Int

  def maxAppendEntriesBatchSize: Int

  def compactionSnapshotCacheTimeToLive: FiniteDuration

  def compactionLogSizeThreshold: Int

  def compactionPreserveLogSize: Int

  def compactionLogSizeCheckInterval: FiniteDuration

  private[raft] def randomizedCompactionLogSizeCheckInterval(): FiniteDuration

  def snapshotSyncCopyingParallelism: Int

  def snapshotSyncPersistenceOperationTimeout: FiniteDuration

  def snapshotSyncMaxSnapshotBatchSize: Int

  def snapshotStoreSnapshotEvery: Int

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

  private[entityreplication] def withJournalPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withSnapshotPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withQueryPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withEventSourcedJournalPluginId(pluginId: String): RaftSettings

  private[entityreplication] def withEventSourcedSnapshotStorePluginId(pluginId: String): RaftSettings

}
