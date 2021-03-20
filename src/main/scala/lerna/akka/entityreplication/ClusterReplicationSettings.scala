package lerna.akka.entityreplication

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.Config
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

object ClusterReplicationSettings {

  def apply(system: ActorSystem): ClusterReplicationSettings = {
    val cluster = Cluster(system)
    new ClusterReplicationSettings(system.settings.config, cluster.settings.Roles)
  }
}

class ClusterReplicationSettings private (root: Config, clusterRoles: Set[String]) {

  val config: Config = root.getConfig("lerna.akka.entityreplication")

  val recoveryEntityTimeout: FiniteDuration = config.getDuration("recovery-entity-timeout").toScala

  val raftSettings: RaftSettings = RaftSettings(root)

  val allMemberIndexes: Set[MemberIndex] = raftSettings.multiRaftRoles.map(MemberIndex.apply)

  val selfMemberIndex: MemberIndex =
    clusterRoles
      .filter(allMemberIndexes.map(_.role)).map(MemberIndex.apply).toSeq match {
      case Seq(memberIndex) => memberIndex
      case Seq() =>
        throw new IllegalStateException(
          s"requires one of ${raftSettings.multiRaftRoles} role",
        )
      case indexes =>
        throw new IllegalStateException(
          s"requires one of ${raftSettings.multiRaftRoles} role, should not have multiply roles: [${indexes.mkString(",")}]",
        )
    }

}
