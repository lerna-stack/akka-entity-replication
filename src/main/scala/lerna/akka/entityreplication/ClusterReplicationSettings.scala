package lerna.akka.entityreplication

import akka.actor.ActorSystem
import com.typesafe.config.Config
import lerna.akka.entityreplication.raft.RaftSettings

import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

object ClusterReplicationSettings {

  def apply(system: ActorSystem): ClusterReplicationSettings = new ClusterReplicationSettings(system.settings.config)
}

class ClusterReplicationSettings(root: Config) {

  val config: Config = root.getConfig("lerna.akka.entityreplication")

  val recoveryEnittyTimeout: FiniteDuration = config.getDuration("recovery-entity-timeout").toScala

  val raftSettings: RaftSettings = RaftSettings(root)
}
