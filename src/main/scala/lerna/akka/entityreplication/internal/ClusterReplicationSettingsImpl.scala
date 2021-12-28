package lerna.akka.entityreplication.internal

import com.typesafe.config.Config
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.typed
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.jdk.DurationConverters._
import scala.concurrent.duration.FiniteDuration

private[entityreplication] final case class ClusterReplicationSettingsImpl(
    config: Config,
    recoveryEntityTimeout: FiniteDuration,
    raftSettings: RaftSettings,
    allMemberIndexes: Set[MemberIndex],
    selfMemberIndex: MemberIndex,
) extends ClusterReplicationSettings
    with typed.ClusterReplicationSettings {

  override def withRaftJournalPluginId(pluginId: String): ClusterReplicationSettingsImpl =
    copy(raftSettings = raftSettings.withJournalPluginId(pluginId))

  override def withRaftSnapshotPluginId(pluginId: String): ClusterReplicationSettingsImpl =
    copy(raftSettings = raftSettings.withSnapshotPluginId(pluginId))

  override def withRaftQueryPluginId(pluginId: String): ClusterReplicationSettingsImpl =
    copy(raftSettings = raftSettings.withQueryPluginId(pluginId))

  override def withEventSourcedJournalPluginId(pluginId: String): ClusterReplicationSettingsImpl =
    copy(raftSettings = raftSettings.withEventSourcedJournalPluginId(pluginId))

  override def withEventSourcedSnapshotStorePluginId(pluginId: String): ClusterReplicationSettingsImpl =
    copy(raftSettings = raftSettings.withEventSourcedSnapshotStorePluginId(pluginId))

}

private[entityreplication] object ClusterReplicationSettingsImpl {

  def apply(root: Config, clusterRoles: Set[String]): ClusterReplicationSettingsImpl = {

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

    ClusterReplicationSettingsImpl(
      config = config,
      recoveryEntityTimeout = recoveryEntityTimeout,
      raftSettings = raftSettings,
      allMemberIndexes = allMemberIndexes,
      selfMemberIndex = selfMemberIndex,
    )
  }
}
