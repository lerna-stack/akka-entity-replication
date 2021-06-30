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
  type Self <: ClusterReplicationSettings
}
