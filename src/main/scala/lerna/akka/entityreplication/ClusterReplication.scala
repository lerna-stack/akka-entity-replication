package lerna.akka.entityreplication

import akka.actor.{ ActorRef, ExtendedActorSystem, Extension, ExtensionId, Props }
import lerna.akka.entityreplication.model.TypeName
import lerna.akka.entityreplication.raft.eventsourced.{ CommitLogStore, ShardedCommitLogStore }

object ClusterReplication extends ExtensionId[ClusterReplication] {

  override def createExtension(system: ExtendedActorSystem): ClusterReplication = new ClusterReplication(system)

  private val actorNamePrefix: String = "replicationRegion"

  private[entityreplication] type EntityPropsProvider = ReplicationActorContext => Props
}

class ClusterReplication private (system: ExtendedActorSystem) extends Extension {

  import ClusterReplication._

  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterReplicationSettings,
      extractEntityId: ReplicationRegion.ExtractEntityId,
      extractShardId: ReplicationRegion.ExtractShardId,
  ): ActorRef = {
    internalStart(typeName, _ => entityProps, settings, extractEntityId, extractShardId)
  }

  private[entityreplication] def internalStart(
      typeName: String,
      entityProps: EntityPropsProvider,
      settings: ClusterReplicationSettings,
      extractEntityId: ReplicationRegion.ExtractEntityId,
      extractShardId: ReplicationRegion.ExtractShardId,
  ): ActorRef = {
    val _typeName = TypeName.from(typeName)

    val maybeCommitLogStore: Option[CommitLogStore] = {
      // TODO: RMUの有効無効をconfigから指定
      val enabled = true // FIXME: settings から取得する (typeName ごとに切り替えられる必要あり)
      // TODO: テストのために差し替え出来るようにする
      Option.when(enabled)(new ShardedCommitLogStore(_typeName, system))
    }

    system.systemActorOf(
      ReplicationRegion.props(typeName, entityProps, settings, extractEntityId, extractShardId, maybeCommitLogStore),
      s"$actorNamePrefix-$typeName",
    )
  }
}
