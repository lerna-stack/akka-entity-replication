package lerna.akka.entityreplication.raft.eventhandler

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings }
import akka.persistence.{ PersistentActor, RecoveryCompleted }
import lerna.akka.entityreplication.raft.model.{ LogEntryIndex, NoOp }

private[eventhandler] final case class Save(
    replicationId: CommitLogStore.ReplicationId,
    index: LogEntryIndex,
    committedEvent: Any,
) extends Serializable

object CommitLogStoreActor {

  def startClusterSharding(typeName: String, system: ActorSystem): ActorRef = {
    val clusterSharding         = ClusterSharding(system)
    val clusterShardingSettings = ClusterShardingSettings(system)

    val messageExtractor = new HashCodeMessageExtractor(maxNumberOfShards = 100) {
      override def entityId(message: Any): String =
        message match {
          case save: Save => save.replicationId
        }
    }

    clusterSharding.start(
      typeName = s"raft-committed-event-store-$typeName",
      entityProps = CommitLogStoreActor.props(typeName),
      settings = clusterShardingSettings,
      messageExtractor = messageExtractor,
    )
  }

  private def props(typeName: String): Props = Props(new CommitLogStoreActor(typeName))
}

class CommitLogStoreActor(typeName: String) extends PersistentActor {
  // TODO: 複数 Raft(typeName) に対応するために typeName ごとに cassandra-journal.keyspace を分ける
  override def journalPluginId: String = "lerna.akka.entityreplication.raft.eventhandler.cassandra-plugin.journal"

  // TODO: Use snapshot for efficient recovery after reboot
  override def snapshotPluginId: String = "akka.persistence.no-snapshot-store"

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

  override def persistenceId: String = self.path.name
}
