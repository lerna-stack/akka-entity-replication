package lerna.akka.entityreplication.raft.eventsourced

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings }
import akka.persistence.{ PersistentActor, RecoveryCompleted }
import akka.util.ByteString
import lerna.akka.entityreplication.{ ClusterReplicationSerializable, ClusterReplicationSettings }
import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }
import lerna.akka.entityreplication.util.ActorIds

import java.net.URLDecoder

private[entityreplication] final case class Save(
    shardId: NormalizedShardId,
    index: LogEntryIndex,
    committedEvent: Any,
) extends ClusterReplicationSerializable

private[entityreplication] object CommitLogStoreActor {

  def startClusterSharding(typeName: TypeName, system: ActorSystem, settings: ClusterReplicationSettings): ActorRef = {
    val clusterSharding         = ClusterSharding(system)
    val clusterShardingSettings = ClusterShardingSettings(system)

    val messageExtractor = new HashCodeMessageExtractor(maxNumberOfShards = 100) {
      override def entityId(message: Any): String =
        message match {
          case save: Save => save.shardId.underlying
          case _          => throw new RuntimeException("unknown message: " + message)
        }
    }

    clusterSharding.start(
      typeName = s"raft-committed-event-store-${typeName.underlying}",
      entityProps = CommitLogStoreActor.props(typeName, settings),
      settings = clusterShardingSettings,
      messageExtractor = messageExtractor,
    )
  }

  def props(typeName: TypeName, settings: ClusterReplicationSettings): Props =
    Props(new CommitLogStoreActor(typeName, settings))

  def persistenceId(typeName: TypeName, shardId: String): String =
    ActorIds.persistenceId("CommitLogStore", typeName.underlying, shardId)

}

private[entityreplication] class CommitLogStoreActor(typeName: TypeName, settings: ClusterReplicationSettings)
    extends PersistentActor {

  override def journalPluginId: String = settings.raftSettings.eventSourcedJournalPluginId

  // TODO: Use snapshot for efficient recovery after reboot
  override def snapshotPluginId: String = "akka.persistence.no-snapshot-store"

  private[this] val shardId = URLDecoder.decode(self.path.name, ByteString.UTF_8)

  // state
  private[this] var currentIndex = LogEntryIndex.initial()

  private[this] def updateState(): Unit = {
    currentIndex = currentIndex.next()
  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
    case _                 => updateState()
  }

  override def receiveCommand: Receive = {
    case save: Save =>
      if (save.index <= currentIndex) {
        // ignore
        sender() ! Done
      } else if (currentIndex.next() == save.index) {
        val event = save.committedEvent match {
          case NoOp        => InternalEvent
          case domainEvent => domainEvent
        }
        persist(event) { _ =>
          updateState()
          sender() ! Done
        }
      } else {
        // 送信側でリトライがあるので新しいindexは無視して、古いindexのeventが再送されるのを待つ
      }
  }

  override def persistenceId: String = CommitLogStoreActor.persistenceId(typeName, shardId)

}
