package lerna.akka.entityreplication.typed

import akka.actor.typed.ActorSystem
import akka.cluster.Cluster
import lerna.akka.entityreplication.internal.ClusterReplicationSettingsImpl
import lerna.akka.{ entityreplication => classic }

object ClusterReplicationSettings {

  def apply(system: ActorSystem[_]): ClusterReplicationSettings = {
    val cluster = Cluster(system)
    ClusterReplicationSettingsImpl(system.settings.config, cluster.settings.Roles)
  }
}

trait ClusterReplicationSettings extends classic.ClusterReplicationSettings {

  /*
   * NOTE:
   * When you changed this API,
   * make sure that we don't have to also change [lerna.akka.entityreplication.ClusterReplicationSettings].
   *
   * This API currently has same API with (classic) ClusterReplicationSettings
   * but 'withXxx' methods override classic one because they should return own type.
   * This trait allows us to use type API by just importing lerna.akka.entityreplication.typed_.
   */

  override def withDisabledShards(disabledShards: Set[String]): ClusterReplicationSettings

  override def withStickyLeaders(stickyLeaders: Map[String, String]): ClusterReplicationSettings

  override def withRaftJournalPluginId(pluginId: String): ClusterReplicationSettings

  override def withRaftSnapshotPluginId(pluginId: String): ClusterReplicationSettings

  override def withRaftQueryPluginId(pluginId: String): ClusterReplicationSettings

  override def withEventSourcedJournalPluginId(pluginId: String): ClusterReplicationSettings

  override def withEventSourcedSnapshotStorePluginId(pluginId: String): ClusterReplicationSettings

}
