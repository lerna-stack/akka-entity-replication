package lerna.akka.entityreplication.raft.eventsourced

import akka.Done
import akka.actor.{ ActorLogging, ActorRef, ActorSystem, Props }
import akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings }
import akka.persistence.{ PersistentActor, RecoveryCompleted, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer }
import akka.util.ByteString
import lerna.akka.entityreplication.{ ClusterReplicationSerializable, ClusterReplicationSettings }
import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex, NoOp }
import lerna.akka.entityreplication.util.ActorIds

import java.net.URLDecoder

@deprecated("Use CommitLogStoreActor.AppendCommittedEntries instead.", "2.1.0")
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
          case save: Save                                     => save.shardId.underlying
          case appendCommittedEntries: AppendCommittedEntries => appendCommittedEntries.shardId.underlying
          case _                                              => throw new RuntimeException("unknown message: " + message)
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

  /** Requests [[CommitLogStoreActor]] to save the events of the entries
    *
    * [[CommitLogStoreActor]] will always reply with an [[AppendCommittedEntriesResponse]] unless it get a failure
    * (such as persistence failures). The response message contains the current index that indices [[CommitLogStoreActor]]
    * already persisted events with indices lower than or equal to that index.
    *
    * Entries should have monotonically increased indices like blow:
    *  - OK: [1,2,3], [2,3,4], or [3]
    *  - NG: [1,3], [1,2,4]
    *
    * If entries don't contains the next entry (which has the current index plus one), the actor will persist nothing and
    * reply with [[AppendCommittedEntriesResponse]]. In this case, the sender has to send another message containing the
    * next entry.
    *
    * If all entries are already persisted, the actor will persist nothing and reply with [[AppendCommittedEntriesResponse]].
    * The current index of the response might be larger than the index the sender know. In this case, the sender can skip
    * some entries (which has indices lower than or equal to the current index) and send another [[AppendCommittedEntries]]
    * containing the next entry.
    *
    * If entries contain already persisted events, the actor will skip such already persisted entries and persist not-persisted
    * events. Note that the entries must contain the next entry. The actor also replies with [[AppendCommittedEntriesResponse]]
    * like in other cases.
    *
    * The entries can be empty. In this case, the actor persists nothing and replies with [[AppendCommittedEntriesResponse]].
    * This case is helpful for the sender who wants to know the current index.
    */
  final case class AppendCommittedEntries(
      shardId: NormalizedShardId,
      entries: Seq[LogEntry],
  ) extends ClusterReplicationSerializable {
    if (entries.nonEmpty) {
      val baseIndex = entries.head.index
      entries.zipWithIndex.foreach {
        case (entry, offset) =>
          require(
            entry.index == baseIndex.plus(offset),
            s"entries should have monotonically increased indices. " +
            s"expected: ${baseIndex.plus(offset)} ($baseIndex+$offset), but got: ${entry.index}",
          )
      }
    }
  }

  /** Response message to [[AppendCommittedEntries]]
    *
    * This message contains the current index after processing the [[AppendCommittedEntries]]. The current index indicates
    * that [[CommitLogStoreActor]] already persisted events with indices lower than or equal to that index.
    */
  final case class AppendCommittedEntriesResponse(currentIndex: LogEntryIndex) extends ClusterReplicationSerializable

  /** State of [[CommitLogStoreActor]]
    *
    * `currentIndex` indicates that [[CommitLogStoreActor]] has persisted events with indices lower than or equal to this
    * index.
    */
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

  import CommitLogStoreActor._

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
    // Don't remove this `Save` command handling for the backward compatibility during rolling update.
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

    case appendCommittedEntries: AppendCommittedEntries =>
      assert(
        appendCommittedEntries.shardId.raw == shardId,
        s"AppendCommittedEntry(shardId=[${appendCommittedEntries.shardId}], [${appendCommittedEntries.entries.size}] entries) " +
        s"should contains the same shard ID [$shardId] of this CommitLogStoreActor",
      )
      receiveAppendCommittedEntries(appendCommittedEntries)

    case SaveSnapshotSuccess(metadata) =>
      log.info("Succeeded to saveSnapshot given metadata [{}]", metadata)

    case SaveSnapshotFailure(metadata, cause) =>
      log.warning(
        "Failed to saveSnapshot given metadata [{}] due to: [{}: {}]",
        metadata,
        cause.getClass.getCanonicalName,
        cause.getMessage,
      )

  }

  override def persistenceId: String = CommitLogStoreActor.persistenceId(typeName, shardId)

  private def receiveAppendCommittedEntries(command: AppendCommittedEntries): Unit = {
    val AppendCommittedEntries(_, entries) = command
    if (entries.isEmpty) {
      sender() ! AppendCommittedEntriesResponse(state.currentIndex)
    } else {
      val hasNoNextEntry      = state.nextIndex < entries.head.index
      val hasNoEntryToPersist = entries.last.index < state.nextIndex
      if (hasNoNextEntry) {
        if (log.isDebugEnabled) {
          log.debug(
            "Ignored all entries of received AppendCommittedEntries([{}] entries) " +
            "since it doesn't contains the next entry (expected next index is [{}], but got the indices [{}..{}]).",
            entries.size,
            state.nextIndex.underlying,
            entries.head.index.underlying,
            entries.last.index.underlying,
          )
        }
        sender() ! AppendCommittedEntriesResponse(state.currentIndex)
      } else if (hasNoEntryToPersist) {
        if (log.isDebugEnabled) {
          log.debug(
            "Ignored all entries of received AppendCommittedEntries([{}] entries) " +
            "since all entries are already persisted (expected next index is [{}], but got the indices [{}..{}]).",
            entries.size,
            state.nextIndex.underlying,
            entries.head.index.underlying,
            entries.last.index.underlying,
          )
        }
        sender() ! AppendCommittedEntriesResponse(state.currentIndex)
      } else {
        if (log.isInfoEnabled) {
          log.info(
            "Received AppendCommittedEntries([{}] entries with indices [{}..{}]).",
            entries.size,
            entries.head.index.underlying,
            entries.last.index.underlying,
          )
        }
        // Copy state.nextIndex since persisting events of entries will update state.nextIndex.
        val nextIndex = state.nextIndex
        // For logging, calculate the number of persisted events.
        var numOfIgnoredEvents   = 0
        var numOfPersistedEvents = 0
        entries.foreach { entry =>
          if (entry.index < nextIndex) {
            if (log.isDebugEnabled) {
              log.debug(
                "Ignored LogEntry(term=[{}], index=[{}], event type=[{}]) since its event is already persisted. Expected next index is [{}].",
                entry.term.term,
                entry.index.underlying,
                entry.event.getClass.getName,
                nextIndex.underlying,
              )
            }
            numOfIgnoredEvents += 1
          } else {
            val event = entry.event.event match {
              case NoOp        => InternalEvent
              case domainEvent => domainEvent
            }
            persist(event) { _ =>
              if (log.isInfoEnabled) {
                log.info(
                  "Persisted event type [{}] of LogEntry(term=[{}], index=[{}]).",
                  event.getClass.getName,
                  entry.term.term,
                  entry.index.underlying,
                )
              }
              numOfPersistedEvents += 1
              updateState()
              if (shouldSaveSnapshot()) {
                saveSnapshot(state)
              }
            }
          }
        }
        // Note that this InternalEvent won't be persisted.
        // `defer` is needed since this actor should reply after all persistence is done.
        defer(InternalEvent) { _ =>
          if (log.isInfoEnabled) {
            log.info(
              "Persisted [{}/{}] events ([{}] events are already persisted).",
              numOfPersistedEvents,
              entries.size,
              numOfIgnoredEvents,
            )
          }
          sender() ! AppendCommittedEntriesResponse(state.currentIndex)
        }
      }
    }
  }

}
