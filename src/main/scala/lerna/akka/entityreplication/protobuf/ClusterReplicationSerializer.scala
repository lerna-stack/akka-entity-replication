package lerna.akka.entityreplication.protobuf

import akka.actor.ExtendedActorSystem
import akka.persistence.query.Offset
import akka.serialization.{ BaseSerializer, SerializationExtension, SerializerWithStringManifest, Serializers }
import com.google.protobuf.ByteString
import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model
import lerna.akka.entityreplication.raft

import java.io.NotSerializableException
import scala.collection.immutable.HashMap

private[entityreplication] final class ClusterReplicationSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private lazy val serialization = SerializationExtension(system)

  // Manifests
  // raft
  private val BegunNewTermManifest          = "AA"
  private val VotedManifest                 = "AB"
  private val DetectedNewTermManifest       = "AC"
  private val AppendedEntriesManifest       = "AD"
  private val AppendedEventManifest         = "AE"
  private val CompactionCompletedManifest   = "AF"
  private val SnapshotSyncCompletedManifest = "AG"
  private val PersistentStateManifest       = "AH"
  private val CommandManifest               = "AI"
  private val ForwardedCommandManifest      = "AJ"
  // raft.eventhandler
  private val CommitLogStoreInternalEventManifest = "BA"
  private val CommitLogStoreSaveManifest          = "BB"
  // raft.protocol
  private val RequestVoteManifest              = "CA"
  private val RequestVoteAcceptedManifest      = "CB"
  private val RequestVoteDeniedManifest        = "CC"
  private val AppendEntriesManifest            = "CD"
  private val AppendEntriesSucceededManifest   = "CE"
  private val AppendEntriesFailedManifest      = "CF"
  private val InstallSnapshotManifest          = "CG"
  private val InstallSnapshotSucceededManifest = "CH"
  private val SuspendEntityManifest            = "CI"
  private val TryCreateEntityManifest          = "CJ"
  // raft.snapshot
  private val EntitySnapshotManifest = "DA"
  // raft.snapshot.sync
  private val SyncCompletedManifest = "EA"
  private val SyncProgressManifest  = "EB"
  // raft.model
  private val NoOpManifest = "FA"

  // Manifest -> fromBinary
  private val fromBinaryMap = HashMap[String, Array[Byte] => ClusterReplicationSerializable](
    // raft
    BegunNewTermManifest          -> begunNewTermFromBinary,
    VotedManifest                 -> votedFromBinary,
    DetectedNewTermManifest       -> detectedNewTermFromBinary,
    AppendedEntriesManifest       -> appendedEntriesFromBinary,
    AppendedEventManifest         -> appendedEventFromBinary,
    CompactionCompletedManifest   -> compactionCompletedFromBinary,
    SnapshotSyncCompletedManifest -> snapshotSyncCompletedFromBinary,
    PersistentStateManifest       -> persistentStateFromBinary,
    CommandManifest               -> commandFromBinary,
    ForwardedCommandManifest      -> forwardedCommandFromBinary,
    // raft.eventhandler
    CommitLogStoreInternalEventManifest -> commitLogStoreInternalEventFromBinary,
    CommitLogStoreSaveManifest          -> commitLogStoreSaveFromBinary,
    // raft.protocol
    RequestVoteManifest              -> requestVoteFromBinary,
    RequestVoteAcceptedManifest      -> requestVoteAcceptedFromBinary,
    RequestVoteDeniedManifest        -> requestVoteDeniedFromBinary,
    AppendEntriesManifest            -> appendEntriesFromBinary,
    AppendEntriesSucceededManifest   -> appendEntriesSucceededFromBinary,
    AppendEntriesFailedManifest      -> appendEntriesFailedFromBinary,
    InstallSnapshotManifest          -> installSnapshotFromBinary,
    InstallSnapshotSucceededManifest -> installSnapshotSucceededFromBinary,
    SuspendEntityManifest            -> suspendEntityFromBinary,
    TryCreateEntityManifest          -> tryCreateEntityFromBinary,
    // raft.snapshot
    EntitySnapshotManifest -> entitySnapshotFromBinary,
    // raft.snapshot.sync
    SyncCompletedManifest -> syncCompletedFromBinary,
    SyncProgressManifest  -> syncProgressFromBinary,
    // raft.model
    NoOpManifest -> noOpFromBinary,
  )

  override def manifest(o: AnyRef): String = {
    o match {
      case message: ClusterReplicationSerializable if serializableToManifest.isDefinedAt(message) =>
        serializableToManifest(message)
      case _ =>
        throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
    }
  }

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case message: ClusterReplicationSerializable if serializableToBinary.isDefinedAt(message) =>
        serializableToBinary(message)
      case _ =>
        throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    fromBinaryMap.get(manifest) match {
      case Some(deserializeFunc) =>
        deserializeFunc(bytes)
      case None =>
        throw new NotSerializableException(
          s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]",
        )
    }
  }

  private val serializableToManifest: PartialFunction[ClusterReplicationSerializable, String] = {
    // raft
    case _: raft.RaftActor.BegunNewTerm              => BegunNewTermManifest
    case _: raft.RaftActor.Voted                     => VotedManifest
    case _: raft.RaftActor.DetectedNewTerm           => DetectedNewTermManifest
    case _: raft.RaftActor.AppendedEntries           => AppendedEntriesManifest
    case _: raft.RaftActor.AppendedEvent             => AppendedEventManifest
    case _: raft.RaftActor.CompactionCompleted       => CompactionCompletedManifest
    case _: raft.RaftActor.SnapshotSyncCompleted     => SnapshotSyncCompletedManifest
    case _: raft.PersistentStateData.PersistentState => PersistentStateManifest
    case _: raft.RaftProtocol.Command                => CommandManifest
    case _: raft.RaftProtocol.ForwardedCommand       => ForwardedCommandManifest
    // raft.eventhandler
    case raft.eventhandler.InternalEvent => CommitLogStoreInternalEventManifest
    case _: raft.eventhandler.Save       => CommitLogStoreSaveManifest
    // raft.protocol
    case _: raft.protocol.RaftCommands.RequestVote              => RequestVoteManifest
    case _: raft.protocol.RaftCommands.RequestVoteAccepted      => RequestVoteAcceptedManifest
    case _: raft.protocol.RaftCommands.RequestVoteDenied        => RequestVoteDeniedManifest
    case _: raft.protocol.RaftCommands.AppendEntries            => AppendEntriesManifest
    case _: raft.protocol.RaftCommands.AppendEntriesSucceeded   => AppendEntriesSucceededManifest
    case _: raft.protocol.RaftCommands.AppendEntriesFailed      => AppendEntriesFailedManifest
    case _: raft.protocol.RaftCommands.InstallSnapshot          => InstallSnapshotManifest
    case _: raft.protocol.RaftCommands.InstallSnapshotSucceeded => InstallSnapshotSucceededManifest
    case _: raft.protocol.SuspendEntity                         => SuspendEntityManifest
    case _: raft.protocol.TryCreateEntity                       => TryCreateEntityManifest
    // raft.snapsnot
    case _: raft.snapshot.SnapshotProtocol.EntitySnapshot => EntitySnapshotManifest
    // raft.snapshot.sync
    case _: raft.snapshot.sync.SnapshotSyncManager.SyncCompleted => SyncCompletedManifest
    case _: raft.snapshot.sync.SnapshotSyncManager.SyncProgress  => SyncProgressManifest
    // raft.model
    case _: raft.model.NoOp.type => NoOpManifest
  }

  private val serializableToBinary: PartialFunction[ClusterReplicationSerializable, Array[Byte]] = {
    // raft
    case m: raft.RaftActor.BegunNewTerm              => begunNewTermToBinary(m)
    case m: raft.RaftActor.Voted                     => votedToBinary(m)
    case m: raft.RaftActor.DetectedNewTerm           => detectedNewTermToBinary(m)
    case m: raft.RaftActor.AppendedEntries           => appendedEntriesToBinary(m)
    case m: raft.RaftActor.AppendedEvent             => appendedEventToBinary(m)
    case m: raft.RaftActor.CompactionCompleted       => compactionCompletedToBinary(m)
    case m: raft.RaftActor.SnapshotSyncCompleted     => snapshotSyncCompletedToBinary(m)
    case m: raft.PersistentStateData.PersistentState => persistentStateToBinary(m)
    case m: raft.RaftProtocol.Command                => commandToBinary(m)
    case m: raft.RaftProtocol.ForwardedCommand       => forwardedCommandToBinary(m)
    // raft.eventhandler
    case m: raft.eventhandler.InternalEvent.type => commitLogStoreInternalEventToBinary(m)
    case m: raft.eventhandler.Save               => commitLogStoreSaveToBinary(m)
    // raft. protocol
    case m: raft.protocol.RaftCommands.RequestVote              => requestVoteToBinary(m)
    case m: raft.protocol.RaftCommands.RequestVoteAccepted      => requestVoteAcceptedToBinary(m)
    case m: raft.protocol.RaftCommands.RequestVoteDenied        => requestVoteDeniedToBinary(m)
    case m: raft.protocol.RaftCommands.AppendEntries            => appendEntriesToBinary(m)
    case m: raft.protocol.RaftCommands.AppendEntriesSucceeded   => appendEntriesSucceededToBinary(m)
    case m: raft.protocol.RaftCommands.AppendEntriesFailed      => appendEntriesFailedToBinary(m)
    case m: raft.protocol.RaftCommands.InstallSnapshot          => installSnapshotToBinary(m)
    case m: raft.protocol.RaftCommands.InstallSnapshotSucceeded => installSnapshotSucceededToBinary(m)
    case m: raft.protocol.SuspendEntity                         => suspendEntityToBinary(m)
    case m: raft.protocol.TryCreateEntity                       => tryCreateEntityToBinary(m)
    // raft.snapshot
    case m: raft.snapshot.SnapshotProtocol.EntitySnapshot => entitySnapShotToBinary(m)
    // raft.snapshot.sync
    case m: raft.snapshot.sync.SnapshotSyncManager.SyncCompleted => syncCompletedToBinary(m)
    case m: raft.snapshot.sync.SnapshotSyncManager.SyncProgress  => syncProgressToBinary(m)
    // raft.model
    case m: raft.model.NoOp.type => noOpToBinary(m)
  }

  // ===
  // raft
  // ===

  private def begunNewTermToBinary(message: raft.RaftActor.BegunNewTerm): Array[Byte] = {
    msg.BegunNewTerm
      .of(
        term = termToProto(message.term),
      ).toByteArray
  }

  private def begunNewTermFromBinary(bytes: Array[Byte]): raft.RaftActor.BegunNewTerm = {
    val proto = msg.BegunNewTerm.parseFrom(bytes)
    raft.RaftActor.BegunNewTerm(
      term = termFromProto(proto.term),
    )
  }

  private def votedToBinary(message: raft.RaftActor.Voted): Array[Byte] = {
    msg.Voted
      .of(
        term = termToProto(message.term),
        candidate = memberIndexToProto(message.candidate),
      ).toByteArray
  }

  private def votedFromBinary(bytes: Array[Byte]): raft.RaftActor.Voted = {
    val proto = msg.Voted.parseFrom(bytes)
    raft.RaftActor.Voted(
      term = termFromProto(proto.term),
      candidate = memberIndexFromProto(proto.candidate),
    )
  }

  private def detectedNewTermToBinary(message: raft.RaftActor.DetectedNewTerm): Array[Byte] = {
    msg.DetectedNewTerm
      .of(
        term = termToProto(message.term),
      ).toByteArray
  }

  private def detectedNewTermFromBinary(bytes: Array[Byte]): raft.RaftActor.DetectedNewTerm = {
    val proto = msg.DetectedNewTerm.parseFrom(bytes)
    raft.RaftActor.DetectedNewTerm(
      term = termFromProto(proto.term),
    )
  }

  private def appendedEntriesToBinary(message: raft.RaftActor.AppendedEntries): Array[Byte] = {
    msg.AppendedEntries
      .of(
        term = termToProto(message.term),
        logEntries = message.logEntries.map(logEntryToProto),
        prevLogIndex = logEntryIndexToProto(message.prevLogIndex),
      ).toByteArray
  }

  private def appendedEntriesFromBinary(bytes: Array[Byte]): raft.RaftActor.AppendedEntries = {
    val proto = msg.AppendedEntries.parseFrom(bytes)
    raft.RaftActor.AppendedEntries(
      term = termFromProto(proto.term),
      logEntries = proto.logEntries.map(logEntryFromProto),
      prevLogIndex = logEntryIndexFromProto(proto.prevLogIndex),
    )
  }

  private def appendedEventToBinary(message: raft.RaftActor.AppendedEvent): Array[Byte] = {
    msg.AppendedEvent
      .of(
        event = entityEventToProto(message.event),
      ).toByteArray
  }

  private def appendedEventFromBinary(bytes: Array[Byte]): raft.RaftActor.AppendedEvent = {
    val proto = msg.AppendedEvent.parseFrom(bytes)
    raft.RaftActor.AppendedEvent(
      event = entityEventFromProto(proto.event),
    )
  }

  private def compactionCompletedToBinary(message: raft.RaftActor.CompactionCompleted): Array[Byte] = {
    msg.CompactionCompleted
      .of(
        memberIndex = memberIndexToProto(message.memberIndex),
        shardId = normalizedShardIdToProto(message.shardId),
        snapshotLastLogTerm = termToProto(message.snapshotLastLogTerm),
        snapshotLastLogIndex = logEntryIndexToProto(message.snapshotLastLogIndex),
        entityIds = message.entityIds.map(normalizedEntityIdToProto).toSeq,
      ).toByteArray
  }

  private def compactionCompletedFromBinary(bytes: Array[Byte]): raft.RaftActor.CompactionCompleted = {
    val proto = msg.CompactionCompleted.parseFrom(bytes)
    raft.RaftActor.CompactionCompleted(
      memberIndex = memberIndexFromProto(proto.memberIndex),
      shardId = normalizedShardIdFromProto(proto.shardId),
      snapshotLastLogTerm = termFromProto(proto.snapshotLastLogTerm),
      snapshotLastLogIndex = logEntryIndexFromProto(proto.snapshotLastLogIndex),
      entityIds = proto.entityIds.map(normalizedEntityIdFromProto).toSet,
    )
  }

  private def snapshotSyncCompletedToBinary(message: raft.RaftActor.SnapshotSyncCompleted): Array[Byte] = {
    msg.SnapshotSyncCompleted
      .of(
        snapshotLastLogTerm = termToProto(message.snapshotLastLogTerm),
        snapshotLastLogIndex = logEntryIndexToProto(message.snapshotLastLogIndex),
      ).toByteArray
  }

  private def snapshotSyncCompletedFromBinary(bytes: Array[Byte]): raft.RaftActor.SnapshotSyncCompleted = {
    val proto = msg.SnapshotSyncCompleted.parseFrom(bytes)
    raft.RaftActor.SnapshotSyncCompleted(
      snapshotLastLogTerm = termFromProto(proto.snapshotLastLogTerm),
      snapshotLastLogIndex = logEntryIndexFromProto(proto.snapshotLastLogIndex),
    )
  }

  private def persistentStateToBinary(message: raft.PersistentStateData.PersistentState): Array[Byte] = {
    msg.PersistentState
      .of(
        currentTerm = termToProto(message.currentTerm),
        replicatedLog = replicatedLogToProto(message.replicatedLog),
        lastSnapshotStatus = snapshotStatusToProto(message.lastSnapshotStatus),
        votedFor = message.votedFor.map(memberIndexToProto),
      ).toByteArray
  }

  private def persistentStateFromBinary(bytes: Array[Byte]): raft.PersistentStateData.PersistentState = {
    val proto = msg.PersistentState.parseFrom(bytes)
    raft.PersistentStateData.PersistentState(
      currentTerm = termFromProto(proto.currentTerm),
      replicatedLog = replicatedLogFromProto(proto.replicatedLog),
      lastSnapshotStatus = snapshotStatusFromProto(proto.lastSnapshotStatus),
      votedFor = proto.votedFor.map(memberIndexFromProto),
    )
  }

  private def commandToBinary(message: raft.RaftProtocol.Command): Array[Byte] = {
    commandToProto(message).toByteArray
  }

  private def commandFromBinary(bytes: Array[Byte]): raft.RaftProtocol.Command = {
    val proto = msg.Command.parseFrom(bytes)
    commandFromProto(proto)
  }

  private def commandToProto(message: raft.RaftProtocol.Command): msg.Command = {
    msg.Command.of(
      command = payloadToProto(message.command),
    )
  }

  private def commandFromProto(proto: msg.Command): raft.RaftProtocol.Command = {
    raft.RaftProtocol.Command(
      command = payloadFromProto(proto.command),
    )
  }

  private def forwardedCommandToBinary(message: raft.RaftProtocol.ForwardedCommand): Array[Byte] = {
    msg.ForwardedCommand
      .of(
        command = commandToProto(message.command),
      ).toByteArray
  }

  private def forwardedCommandFromBinary(bytes: Array[Byte]): raft.RaftProtocol.ForwardedCommand = {
    val proto = msg.ForwardedCommand.parseFrom(bytes)
    raft.RaftProtocol.ForwardedCommand(
      command = commandFromProto(proto.command),
    )
  }

  // ===
  // raft.eventhandler
  // ===

  private def commitLogStoreInternalEventToBinary(message: raft.eventhandler.InternalEvent.type): Array[Byte] = {
    // Use a consistent style for the future even if InternalEvent has no fields.
    msg.CommitLogStoreInternalEvent.of().toByteArray
  }

  private def commitLogStoreInternalEventFromBinary(bytes: Array[Byte]): raft.eventhandler.InternalEvent.type = {
    // Use a consistent style for the future even if InternalEvent has no fields.
    val _ = msg.CommitLogStoreInternalEvent.parseFrom(bytes)
    raft.eventhandler.InternalEvent
  }

  private def commitLogStoreSaveToBinary(message: raft.eventhandler.Save): Array[Byte] = {
    msg.CommitLogStoreSave
      .of(
        replicationId = replicationIdToProto(message.replicationId),
        index = logEntryIndexToProto(message.index),
        committedEvent = payloadToProto(message.committedEvent),
      ).toByteArray
  }

  private def commitLogStoreSaveFromBinary(bytes: Array[Byte]): raft.eventhandler.Save = {
    val proto = msg.CommitLogStoreSave.parseFrom(bytes)
    raft.eventhandler.Save(
      replicationId = replicationIdFromProto(proto.replicationId),
      index = logEntryIndexFromProto(proto.index),
      committedEvent = payloadFromProto(proto.committedEvent),
    )
  }

  private def replicationIdToProto(message: raft.eventhandler.CommitLogStore.ReplicationId): msg.ReplicationId = {
    // Use a consistent style for the future even if the ReplicationId is not a value class
    msg.ReplicationId.of(message)
  }

  private def replicationIdFromProto(proto: msg.ReplicationId): raft.eventhandler.CommitLogStore.ReplicationId = {
    // Use a consistent style for the future even if the ReplicationId is not a value class
    proto.underlying
  }

  // ===
  // raft.protocol
  // ===

  private def requestVoteToBinary(message: raft.protocol.RaftCommands.RequestVote): Array[Byte] = {
    msg.RequestVote
      .of(
        shardId = normalizedShardIdToProto(message.shardId),
        term = termToProto(message.term),
        candidate = memberIndexToProto(message.candidate),
        lastLogIndex = logEntryIndexToProto(message.lastLogIndex),
        lastLogTerm = termToProto(message.lastLogTerm),
      ).toByteArray
  }

  private def requestVoteFromBinary(bytes: Array[Byte]): raft.protocol.RaftCommands.RequestVote = {
    val proto = msg.RequestVote.parseFrom(bytes)
    raft.protocol.RaftCommands.RequestVote(
      shardId = normalizedShardIdFromProto(proto.shardId),
      term = termFromProto(proto.term),
      candidate = memberIndexFromProto(proto.candidate),
      lastLogIndex = logEntryIndexFromProto(proto.lastLogIndex),
      lastLogTerm = termFromProto(proto.lastLogTerm),
    )
  }

  private def requestVoteAcceptedToBinary(message: raft.protocol.RaftCommands.RequestVoteAccepted): Array[Byte] = {
    msg.RequestVoteAccepted
      .of(
        term = termToProto(message.term),
        sender = memberIndexToProto(message.sender),
      ).toByteArray
  }

  private def requestVoteAcceptedFromBinary(bytes: Array[Byte]): raft.protocol.RaftCommands.RequestVoteAccepted = {
    val proto = msg.RequestVoteAccepted.parseFrom(bytes)
    raft.protocol.RaftCommands.RequestVoteAccepted(
      term = termFromProto(proto.term),
      sender = memberIndexFromProto(proto.sender),
    )
  }

  private def requestVoteDeniedToBinary(message: raft.protocol.RaftCommands.RequestVoteDenied): Array[Byte] = {
    msg.RequestVoteDenied
      .of(
        term = termToProto(message.term),
      ).toByteArray
  }

  private def requestVoteDeniedFromBinary(bytes: Array[Byte]): raft.protocol.RaftCommands.RequestVoteDenied = {
    val proto = msg.RequestVoteDenied.parseFrom(bytes)
    raft.protocol.RaftCommands.RequestVoteDenied(
      term = termFromProto(proto.term),
    )
  }

  private def appendEntriesToBinary(message: raft.protocol.RaftCommands.AppendEntries): Array[Byte] = {
    msg.AppendEntries
      .of(
        shardId = normalizedShardIdToProto(message.shardId),
        term = termToProto(message.term),
        leader = memberIndexToProto(message.leader),
        prevLogIndex = logEntryIndexToProto(message.prevLogIndex),
        prevLogTerm = termToProto(message.prevLogTerm),
        entries = message.entries.map(logEntryToProto),
        leaderCommit = logEntryIndexToProto(message.leaderCommit),
      ).toByteArray
  }

  private def appendEntriesFromBinary(bytes: Array[Byte]): raft.protocol.RaftCommands.AppendEntries = {
    val proto = msg.AppendEntries.parseFrom(bytes)
    raft.protocol.RaftCommands.AppendEntries(
      shardId = normalizedShardIdFromProto(proto.shardId),
      term = termFromProto(proto.term),
      leader = memberIndexFromProto(proto.leader),
      prevLogIndex = logEntryIndexFromProto(proto.prevLogIndex),
      prevLogTerm = termFromProto(proto.prevLogTerm),
      entries = proto.entries.map(logEntryFromProto),
      leaderCommit = logEntryIndexFromProto(proto.leaderCommit),
    )
  }

  private def appendEntriesSucceededToBinary(
      message: raft.protocol.RaftCommands.AppendEntriesSucceeded,
  ): Array[Byte] = {
    msg.AppendEntriesSucceeded
      .of(
        term = termToProto(message.term),
        lastLogIndex = logEntryIndexToProto(message.lastLogIndex),
        sender = memberIndexToProto(message.sender),
      ).toByteArray
  }

  private def appendEntriesSucceededFromBinary(
      bytes: Array[Byte],
  ): raft.protocol.RaftCommands.AppendEntriesSucceeded = {
    val proto = msg.AppendEntriesSucceeded.parseFrom(bytes)
    raft.protocol.RaftCommands.AppendEntriesSucceeded(
      term = termFromProto(proto.term),
      lastLogIndex = logEntryIndexFromProto(proto.lastLogIndex),
      sender = memberIndexFromProto(proto.sender),
    )
  }

  private def appendEntriesFailedToBinary(message: raft.protocol.RaftCommands.AppendEntriesFailed): Array[Byte] = {
    msg.AppendEntriesFailed
      .of(
        term = termToProto(message.term),
        sender = memberIndexToProto(message.sender),
      ).toByteArray
  }

  private def appendEntriesFailedFromBinary(bytes: Array[Byte]): raft.protocol.RaftCommands.AppendEntriesFailed = {
    val proto = msg.AppendEntriesFailed.parseFrom(bytes)
    raft.protocol.RaftCommands.AppendEntriesFailed(
      term = termFromProto(proto.term),
      sender = memberIndexFromProto(proto.sender),
    )
  }

  private def installSnapshotToBinary(message: raft.protocol.RaftCommands.InstallSnapshot): Array[Byte] = {
    msg.InstallSnapshot
      .of(
        shardId = normalizedShardIdToProto(message.shardId),
        term = termToProto(message.term),
        srcMemberIndex = memberIndexToProto(message.srcMemberIndex),
        srcLatestSnapshotLastLogTerm = termToProto(message.srcLatestSnapshotLastLogTerm),
        srcLatestSnapshotLastLogLogIndex = logEntryIndexToProto(message.srcLatestSnapshotLastLogLogIndex),
      ).toByteArray
  }

  private def installSnapshotFromBinary(bytes: Array[Byte]): raft.protocol.RaftCommands.InstallSnapshot = {
    val proto = msg.InstallSnapshot.parseFrom(bytes)
    raft.protocol.RaftCommands.InstallSnapshot(
      shardId = normalizedShardIdFromProto(proto.shardId),
      term = termFromProto(proto.term),
      srcMemberIndex = memberIndexFromProto(proto.srcMemberIndex),
      srcLatestSnapshotLastLogTerm = termFromProto(proto.srcLatestSnapshotLastLogTerm),
      srcLatestSnapshotLastLogLogIndex = logEntryIndexFromProto(proto.srcLatestSnapshotLastLogLogIndex),
    )
  }

  private def installSnapshotSucceededToBinary(
      message: raft.protocol.RaftCommands.InstallSnapshotSucceeded,
  ): Array[Byte] = {
    msg.InstallSnapshotSucceeded
      .of(
        shardId = normalizedShardIdToProto(message.shardId),
        term = termToProto(message.term),
        dstLatestSnapshotLastLogLogIndex = logEntryIndexToProto(message.dstLatestSnapshotLastLogLogIndex),
        sender = memberIndexToProto(message.sender),
      ).toByteArray
  }

  private def installSnapshotSucceededFromBinary(
      bytes: Array[Byte],
  ): raft.protocol.RaftCommands.InstallSnapshotSucceeded = {
    val proto = msg.InstallSnapshotSucceeded.parseFrom(bytes)
    raft.protocol.RaftCommands.InstallSnapshotSucceeded(
      shardId = normalizedShardIdFromProto(proto.shardId),
      term = termFromProto(proto.term),
      dstLatestSnapshotLastLogLogIndex = logEntryIndexFromProto(proto.dstLatestSnapshotLastLogLogIndex),
      sender = memberIndexFromProto(proto.sender),
    )
  }

  private def suspendEntityToBinary(message: raft.protocol.SuspendEntity): Array[Byte] = {
    msg.SuspendEntity
      .of(
        shardId = normalizedShardIdToProto(message.shardId),
        entityId = normalizedEntityIdToProto(message.entityId),
        stopMessage = payloadToProto(message.stopMessage),
      ).toByteArray
  }

  private def suspendEntityFromBinary(bytes: Array[Byte]): raft.protocol.SuspendEntity = {
    val proto = msg.SuspendEntity.parseFrom(bytes)
    raft.protocol.SuspendEntity(
      shardId = normalizedShardIdFromProto(proto.shardId),
      entityId = normalizedEntityIdFromProto(proto.entityId),
      stopMessage = payloadFromProto(proto.stopMessage),
    )
  }

  private def tryCreateEntityToBinary(message: raft.protocol.TryCreateEntity): Array[Byte] = {
    msg.TryCreateEntity
      .of(
        shardId = normalizedShardIdToProto(message.shardId),
        entityId = normalizedEntityIdToProto(message.entityId),
      ).toByteArray
  }

  private def tryCreateEntityFromBinary(bytes: Array[Byte]): raft.protocol.TryCreateEntity = {
    val proto = msg.TryCreateEntity.parseFrom(bytes)
    raft.protocol.TryCreateEntity(
      shardId = normalizedShardIdFromProto(proto.shardId),
      entityId = normalizedEntityIdFromProto(proto.entityId),
    )
  }

  // ===
  // raft.snapshot
  // ===

  private def entitySnapShotToBinary(message: raft.snapshot.SnapshotProtocol.EntitySnapshot): Array[Byte] = {
    msg.EntitySnapshot
      .of(
        metadata = entitySnapshotMetadataToProto(message.metadata),
        state = entityStateToProto(message.state),
      ).toByteArray
  }

  private def entitySnapshotFromBinary(bytes: Array[Byte]): raft.snapshot.SnapshotProtocol.EntitySnapshot = {
    val proto = msg.EntitySnapshot.parseFrom(bytes)
    raft.snapshot.SnapshotProtocol.EntitySnapshot(
      metadata = entitySnapshotMetadataFromProto(proto.metadata),
      state = entityStateFromProto(proto.state),
    )
  }

  private def entitySnapshotMetadataToProto(
      message: raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata,
  ): msg.EntitySnapshotMetadata = {
    msg.EntitySnapshotMetadata.of(
      entityId = normalizedEntityIdToProto(message.entityId),
      logEntryIndex = logEntryIndexToProto(message.logEntryIndex),
    )
  }

  private def entitySnapshotMetadataFromProto(
      proto: msg.EntitySnapshotMetadata,
  ): raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata = {
    raft.snapshot.SnapshotProtocol.EntitySnapshotMetadata(
      entityId = normalizedEntityIdFromProto(proto.entityId),
      logEntryIndex = logEntryIndexFromProto(proto.logEntryIndex),
    )
  }

  private def entityStateToProto(message: raft.snapshot.SnapshotProtocol.EntityState): msg.EntityState = {
    msg.EntityState.of(
      underlying = payloadToProto(message.underlying),
    )
  }

  private def entityStateFromProto(proto: msg.EntityState): raft.snapshot.SnapshotProtocol.EntityState = {
    raft.snapshot.SnapshotProtocol.EntityState(
      underlying = payloadFromProto(proto.underlying),
    )
  }

  // ===
  // raft.snapshot.sync
  // ===

  private def syncCompletedToBinary(message: raft.snapshot.sync.SnapshotSyncManager.SyncCompleted): Array[Byte] = {
    msg.SyncCompleted
      .of(
        offset = offsetToProto(message.offset),
      ).toByteArray
  }

  private def syncCompletedFromBinary(bytes: Array[Byte]): raft.snapshot.sync.SnapshotSyncManager.SyncCompleted = {
    val proto = msg.SyncCompleted.parseFrom(bytes)
    raft.snapshot.sync.SnapshotSyncManager.SyncCompleted(
      offset = offsetFromProto(proto.offset),
    )
  }

  private def syncProgressToBinary(message: raft.snapshot.sync.SnapshotSyncManager.SyncProgress): Array[Byte] = {
    msg.SyncProgress
      .of(
        offset = offsetToProto(message.offset),
      ).toByteArray
  }

  private def syncProgressFromBinary(bytes: Array[Byte]): raft.snapshot.sync.SnapshotSyncManager.SyncProgress = {
    val proto = msg.SyncProgress.parseFrom(bytes)
    raft.snapshot.sync.SnapshotSyncManager.SyncProgress(
      offset = offsetFromProto(proto.offset),
    )
  }

  private def offsetToProto(message: Offset): msg.Offset = {
    // Use a serializer defined in akka.persistence.query
    msg.Offset.of(
      underlying = payloadToProto(message),
    )
  }

  private def offsetFromProto(proto: msg.Offset): Offset = {
    // Use a serializer defined in akka.persistence.query
    payloadFromProto(proto.underlying) match {
      case offset: Offset => offset
      case _ =>
        throw new NotSerializableException(
          s"Unexpected deserialization of Offset in [${getClass.getName}]",
        )
    }
  }

  // ===
  // model
  // ===

  private def normalizedEntityIdToProto(message: model.NormalizedEntityId): msg.NormalizedEntityId = {
    msg.NormalizedEntityId.of(
      underlying = message.underlying,
    )
  }

  private def normalizedEntityIdFromProto(proto: msg.NormalizedEntityId): model.NormalizedEntityId = {
    model.NormalizedEntityId.fromEncodedValue(
      encodedEntityId = proto.underlying,
    )
  }

  private def normalizedShardIdToProto(message: model.NormalizedShardId): msg.NormalizedShardId = {
    msg.NormalizedShardId.of(
      underlying = message.underlying,
    )
  }

  private def normalizedShardIdFromProto(proto: msg.NormalizedShardId): model.NormalizedShardId = {
    model.NormalizedShardId.fromEncodedValue(
      encodedShardId = proto.underlying,
    )
  }

  // ===
  // raft.model
  // ===

  private def noOpToBinary(message: raft.model.NoOp.type): Array[Byte] = {
    // Use a consistent style for the future even if NoOp has no fields.
    msg.NoOp.of().toByteArray
  }

  private def noOpFromBinary(bytes: Array[Byte]): raft.model.NoOp.type = {
    // use a consistent style for the future even if NoOp has no fields.
    val _ = msg.CommitLogStoreInternalEvent.parseFrom(bytes)
    raft.model.NoOp
  }

  private def entityEventToProto(message: raft.model.EntityEvent): msg.EntityEvent = {
    msg.EntityEvent.of(
      event = payloadToProto(message.event),
      entityId = message.entityId.map(normalizedEntityIdToProto),
    )
  }

  private def entityEventFromProto(proto: msg.EntityEvent): raft.model.EntityEvent = {
    raft.model.EntityEvent(
      entityId = proto.entityId.map(normalizedEntityIdFromProto),
      event = payloadFromProto(proto.event),
    )
  }

  private def logEntryToProto(message: raft.model.LogEntry): msg.LogEntry = {
    msg.LogEntry.of(
      index = logEntryIndexToProto(message.index),
      event = entityEventToProto(message.event),
      term = termToProto(message.term),
    )
  }

  private def logEntryFromProto(proto: msg.LogEntry): raft.model.LogEntry = {
    raft.model.LogEntry(
      index = logEntryIndexFromProto(proto.index),
      event = entityEventFromProto(proto.event),
      term = termFromProto(proto.term),
    )
  }

  private def logEntryIndexToProto(message: raft.model.LogEntryIndex): msg.LogEntryIndex = {
    msg.LogEntryIndex.of(
      underlying = message.underlying,
    )
  }

  private def logEntryIndexFromProto(proto: msg.LogEntryIndex): raft.model.LogEntryIndex = {
    raft.model.LogEntryIndex(
      underlying = proto.underlying,
    )
  }

  private def replicatedLogToProto(message: raft.model.ReplicatedLog): msg.ReplicatedLog = {
    msg.ReplicatedLog.of(
      entries = message.entries.map(logEntryToProto),
      ancestorLastTerm = termToProto(message.ancestorLastTerm),
      ancestorLastIndex = logEntryIndexToProto(message.ancestorLastIndex),
    )
  }

  private def replicatedLogFromProto(proto: msg.ReplicatedLog): raft.model.ReplicatedLog = {
    raft.model.ReplicatedLog(
      entries = proto.entries.map(logEntryFromProto),
      ancestorLastTerm = termFromProto(proto.ancestorLastTerm),
      ancestorLastIndex = logEntryIndexFromProto(proto.ancestorLastIndex),
    )
  }

  private def snapshotStatusToProto(message: raft.model.SnapshotStatus): msg.SnapshotStatus = {
    msg.SnapshotStatus.of(
      snapshotLastTerm = termToProto(message.snapshotLastTerm),
      snapshotLastLogIndex = logEntryIndexToProto(message.snapshotLastLogIndex),
    )
  }

  private def snapshotStatusFromProto(proto: msg.SnapshotStatus): raft.model.SnapshotStatus = {
    raft.model.SnapshotStatus(
      snapshotLastTerm = termFromProto(proto.snapshotLastTerm),
      snapshotLastLogIndex = logEntryIndexFromProto(proto.snapshotLastLogIndex),
    )
  }

  private def termToProto(message: raft.model.Term): msg.Term = {
    msg.Term.of(
      term = message.term,
    )
  }

  private def termFromProto(proto: msg.Term): raft.model.Term = {
    raft.model.Term(
      term = proto.term,
    )
  }

  // ===
  // raft.routing
  // ===

  private def memberIndexToProto(message: raft.routing.MemberIndex): msg.MemberIndex = {
    msg.MemberIndex.of(
      role = message.role,
    )
  }

  private def memberIndexFromProto(proto: msg.MemberIndex): raft.routing.MemberIndex = {
    raft.routing.MemberIndex.fromEncodedValue(
      encodedRole = proto.role,
    )
  }

  // ===
  // payload
  // ===

  private def payloadToProto(message: Any): msg.Payload = {
    val messageRef      = message.asInstanceOf[AnyRef]
    val serializer      = serialization.findSerializerFor(messageRef)
    val enclosedMessage = ByteString.copyFrom(serializer.toBinary(messageRef))
    val serializerId    = serializer.identifier
    val manifest        = ByteString.copyFromUtf8(Serializers.manifestFor(serializer, messageRef))
    msg.Payload.of(
      enclosedMessage = enclosedMessage,
      serializerId = serializerId,
      messageManifest = Option(manifest),
    )
  }

  private def payloadFromProto(payload: msg.Payload): AnyRef = {
    val manifest        = payload.messageManifest.fold("")(_.toStringUtf8)
    val enclosedMessage = payload.enclosedMessage.toByteArray
    val serializerId    = payload.serializerId
    serialization.deserialize(enclosedMessage, serializerId, manifest).get
  }

}
