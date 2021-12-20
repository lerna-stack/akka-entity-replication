package lerna.akka.entityreplication.raft.eventsourced

import akka.Done
import akka.actor.{ ActorLogging, ActorRef, ActorSystem, Props }
import akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings }
import akka.persistence.{ PersistentActor, RecoveryCompleted, SnapshotOffer }
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

  /** State of [[CommitLogStoreActor]] */
  final case class State(currentIndex: LogEntryIndex) extends ClusterReplicationSerializable {
    def nextIndex: LogEntryIndex = {
      currentIndex.next()
    }
    def next: State = {
      copy(currentIndex.next())
    }
  }
  object State {
    def apply(): State = State(LogEntryIndex.initial())
  }

}

private[entityreplication] class CommitLogStoreActor(typeName: TypeName, settings: ClusterReplicationSettings)
    extends PersistentActor
    with ActorLogging {

  import CommitLogStoreActor.State

  override def journalPluginId: String = settings.raftSettings.eventSourcedJournalPluginId

  override def snapshotPluginId: String = settings.raftSettings.eventSourcedSnapshotStorePluginId

  private[this] val shardId = URLDecoder.decode(self.path.name, ByteString.UTF_8)

  private val snapshotInterval: Int = settings.raftSettings.eventSourcedSnapshotEvery

  private var state: State = State()

  private[this] def updateState(): Unit = {
    state = state.next
  }

  private def shouldSaveSnapshot(): Boolean = {
    lastSequenceNr % snapshotInterval == 0 && // save a snapshot every interval
    lastSequenceNr > 0 // should have at-least one event
  }

  override def receiveRecover: Receive = {
    case SnapshotOffer(metadata, snapshot: State) =>
      log.info("Loaded snapshot [{}] with metadata [{}]", snapshot, metadata)
      state = snapshot
    case RecoveryCompleted =>
    case _                 => updateState()
  }

  override def receiveCommand: Receive = {
    case save: Save =>
      val expectedIndex = state.nextIndex
      if (save.index < expectedIndex) {
        // ignore
        sender() ! Done
      } else if (save.index == expectedIndex) {
        val event = save.committedEvent match {
          case NoOp        => InternalEvent
          case domainEvent => domainEvent
        }
        persist(event) { _ =>
          updateState()
          if (shouldSaveSnapshot()) {
            saveSnapshot(state)
          }
          sender() ! Done
        }
      } else {
        // 送信側でリトライがあるので新しいindexは無視して、古いindexのeventが再送されるのを待つ
      }
  }

  override def persistenceId: String = CommitLogStoreActor.persistenceId(typeName, shardId)

}
