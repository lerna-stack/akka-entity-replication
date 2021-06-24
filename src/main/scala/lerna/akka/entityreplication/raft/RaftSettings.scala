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

  def maxAppendEntriesSize: Int

  def maxAppendEntriesBatchSize: Int

  def compactionSnapshotCacheTimeToLive: FiniteDuration

  def compactionLogSizeThreshold: Int

  def compactionPreserveLogSize: Int

  def compactionLogSizeCheckInterval: FiniteDuration

  private[raft] def randomizedCompactionLogSizeCheckInterval(): FiniteDuration

  def snapshotSyncCopyingParallelism: Int

  def snapshotSyncPersistenceOperationTimeout: FiniteDuration

  def clusterShardingConfig: Config

  def journalPluginId: String

  def journalPluginAdditionalConfig: Config

  def snapshotStorePluginId: String

  def queryPluginId: String

  def eventSourcedJournalPluginId: String
}
