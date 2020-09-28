package lerna.akka.entityreplication

import akka.actor.{ ActorRef, ActorSystem, Props }
import lerna.akka.entityreplication.raft.eventhandler.{
  CommitLogStore,
  EventHandler,
  EventHandlerSingleton,
  ShardedCommitLogStore,
}

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
      maybeEventHandler: Option[EventHandler] = None,
  ): ActorRef = {
    val maybeCommitLogStore: Option[CommitLogStore] = {
      // TODO: RMUの有効無効をconfigから指定
      val enabled = true // FIXME: settings から取得する (typeName ごとに切り替えられる必要あり)
      // TODO: テストのために差し替え出来るようにする
      if (enabled) Option(new ShardedCommitLogStore(typeName, system))
      else None
    }
    maybeEventHandler match {
      case Some(eventHandler) =>
        if (maybeCommitLogStore.isEmpty) {
          // TODO: warn log 出力: 「committed event の保存が有効でないので eventHandler に event が流れることはない
        }
        EventHandlerSingleton.startAsSingleton(typeName, system, eventHandler)
      case None => // noop
    }

    system.actorOf(
      ReplicationRegion.props(typeName, entityProps, settings, extractEntityId, extractShardId, maybeCommitLogStore),
      s"$actorNamePrefix-$typeName",
    )
  }

  def replicationRegion(typeName: String): ActorRef = ???
}
