package lerna.akka.entityreplication

import akka.actor.{ ActorRef, ActorSystem, Props }
import lerna.akka.entityreplication.raft.eventhandler.{ CommitLogStore, ShardedCommitLogStore }

object ClusterReplication {

  def apply(system: ActorSystem): ClusterReplication = new ClusterReplication(system)

  val actorNamePrefix: String = "replicationRegion"
}

class ClusterReplication(system: ActorSystem) {

  import ClusterReplication._

  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterReplicationSettings,
      extractEntityId: ReplicationRegion.ExtractEntityId,
      extractShardId: ReplicationRegion.ExtractShardId,
  ): ActorRef = {
    val maybeCommitLogStore: Option[CommitLogStore] = {
      // TODO: RMUの有効無効をconfigから指定
      val enabled = true // FIXME: settings から取得する (typeName ごとに切り替えられる必要あり)
      // TODO: テストのために差し替え出来るようにする
      Option.when(enabled)(new ShardedCommitLogStore(typeName, system))
    }

    system.actorOf(
      ReplicationRegion.props(typeName, entityProps, settings, extractEntityId, extractShardId, maybeCommitLogStore),
      s"$actorNamePrefix-$typeName",
    )
  }
}
