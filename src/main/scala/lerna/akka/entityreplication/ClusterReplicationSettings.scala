package lerna.akka.entityreplication

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.Config
import lerna.akka.entityreplication.internal.ClusterReplicationSettingsImpl
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.concurrent.duration.FiniteDuration

object ClusterReplicationSettings {

  @deprecated("Use typed.ClusterReplicationSettings instead", since = "2.0.0")
  def apply(system: ActorSystem): ClusterReplicationSettings = create(system)

  // for internal use
  private[entityreplication] def create(system: ActorSystem): ClusterReplicationSettings = {
    val cluster = Cluster(system)
    ClusterReplicationSettingsImpl(system.settings.config, cluster.settings.Roles)
  }
}

trait ClusterReplicationSettings {

  /*
   * NOTE:
   * When you changed this API,
   * make sure that we don't have to also change [lerna.akka.entityreplication.typed.ClusterReplicationSettings].
   */

  def config: Config

  def recoveryEntityTimeout: FiniteDuration

  def raftSettings: RaftSettings

  def allMemberIndexes: Set[MemberIndex]

  def selfMemberIndex: MemberIndex

  def withDisabledShards(disabledShards: Set[String]): ClusterReplicationSettings

  def withRaftJournalPluginId(pluginId: String): ClusterReplicationSettings

  def withRaftSnapshotPluginId(pluginId: String): ClusterReplicationSettings

  def withRaftQueryPluginId(pluginId: String): ClusterReplicationSettings

  def withEventSourcedJournalPluginId(pluginId: String): ClusterReplicationSettings

  def withEventSourcedSnapshotStorePluginId(pluginId: String): ClusterReplicationSettings

}
