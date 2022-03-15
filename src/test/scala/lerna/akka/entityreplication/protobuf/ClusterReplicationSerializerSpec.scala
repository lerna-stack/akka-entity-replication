package lerna.akka.entityreplication.protobuf

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.persistence.query.{ NoOffset, Sequence, TimeBasedUUID }
import lerna.akka.entityreplication.ClusterReplicationSerializable
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.protobuf.ClusterReplicationSerializerSpec.{
  MyCommand,
  MyEntity,
  MyEvent,
  MyStopMessage,
}
import lerna.akka.entityreplication.raft.PersistentStateData.PersistentState
import lerna.akka.entityreplication.raft.RaftActor._
import lerna.akka.entityreplication.raft.RaftProtocol.{ Command, ForwardedCommand }
import lerna.akka.entityreplication.raft.eventsourced.{ CommitLogStoreActor, InternalEvent, Save }
import lerna.akka.entityreplication.raft.model.{
  EntityEvent,
  LogEntry,
  LogEntryIndex,
  NoOp,
  ReplicatedLog,
  SnapshotStatus,
  Term,
}
import lerna.akka.entityreplication.raft.protocol.RaftCommands.{
  AppendEntries,
  AppendEntriesFailed,
  AppendEntriesSucceeded,
  InstallSnapshot,
  InstallSnapshotSucceeded,
  RequestVote,
  RequestVoteAccepted,
  RequestVoteDenied,
}
import lerna.akka.entityreplication.raft.protocol.{ SuspendEntity, TryCreateEntity }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.{
  EntitySnapshot,
  EntitySnapshotMetadata,
  EntityState,
}
import lerna.akka.entityreplication.raft.snapshot.sync.SnapshotSyncManager.{
  SnapshotCopied,
  SyncCompleted,
  SyncProgress,
}
import lerna.akka.entityreplication.testkit.KryoSerializable
import lerna.akka.entityreplication.typed.ReplicationEnvelope

import java.io.NotSerializableException
import java.util.UUID
import scala.annotation.nowarn

object ClusterReplicationSerializerSpec {
  case class MyEntity(id: Long, name: String, age: Int) extends KryoSerializable
  case class MyCommand(id: Long, message: String)       extends KryoSerializable
  case class MyEvent(id: Long, message: String)         extends KryoSerializable
  object MyStopMessage                                  extends KryoSerializable
}

@nowarn("msg=Use CommitLogStoreActor.AppendCommittedEntries instead.")
final class ClusterReplicationSerializerSpec
    extends SerializerSpecBase(ActorSystem("ClusterReplicationSerializerSpec")) {

  private val serializer = new ClusterReplicationSerializer(system.asInstanceOf[ExtendedActorSystem])

  private def checkSerialization(message: ClusterReplicationSerializable): Unit = {
    val blob = serializer.toBinary(message)
    val ref  = serializer.fromBinary(blob, serializer.manifest(message))
    ref shouldBe message
  }

  "serialize an instance of ClusterReplicationSerializable" in {
    // raft
    checkSerialization(BegunNewTerm(Term(12345)))
    checkSerialization(Voted(Term(234), MemberIndex("need&url%encode")))
    checkSerialization(DetectedNewTerm(Term(8417)))
    checkSerialization(
      AppendedEntries(
        Term(12851),
        Seq(
          LogEntry(
            LogEntryIndex(2),
            EntityEvent(None, MyEvent(2141, "message&hello")),
            Term(9841),
          ),
          LogEntry(
            LogEntryIndex(2),
            EntityEvent(Some(NormalizedEntityId.from("shard:1248")), MyEvent(5891, "message?world")),
            Term(9841),
          ),
        ),
        LogEntryIndex(1),
      ),
    )
    checkSerialization(
      AppendedEvent(
        EntityEvent(Some(NormalizedEntityId.from("s14")), MyEvent(741, "message!hello")),
      ),
    )
    checkSerialization(
      CompactionCompleted(
        MemberIndex("member&need%url?encode"),
        NormalizedShardId.from("shard&need%url?encode"),
        Term(85714),
        LogEntryIndex(2357),
        Set(
          NormalizedEntityId.from("shard1"),
          NormalizedEntityId.from("shard2&need%url?encode"),
          NormalizedEntityId.from("shard3"),
        ),
      ),
    )
    checkSerialization(
      SnapshotSyncCompleted(
        Term(12831),
        LogEntryIndex(1238),
      ),
    )
    checkSerialization(
      PersistentState(
        Term(841),
        Some(MemberIndex("member&index")),
        ReplicatedLog(
          Seq(
            LogEntry(
              LogEntryIndex(81),
              EntityEvent(None, MyEvent(12, "message?state&")),
              Term(8265),
            ),
          ),
          Term(2781),
          LogEntryIndex(12305),
        ),
        SnapshotStatus(Term(820), LogEntryIndex(9751)),
      ),
    )
    checkSerialization(Command(MyCommand(112947, "big")))
    checkSerialization(ForwardedCommand(Command(MyCommand(19472, "bang"))))

    // raft.eventsourced
    checkSerialization(InternalEvent)
    checkSerialization(
      Save(
        NormalizedShardId.from("shard:need?url&encode"),
        LogEntryIndex(75185),
        MyEvent(908125, "save?my-event!"),
      ),
    )
    checkSerialization(CommitLogStoreActor.State(LogEntryIndex(6451)))
    checkSerialization(
      CommitLogStoreActor.AppendCommittedEntries(
        NormalizedShardId.from("shard&need%url?encode"),
        Seq(
          LogEntry(
            LogEntryIndex(3),
            EntityEvent(Option(NormalizedEntityId("entity1")), MyEvent(21, "my-event:abc:127")),
            Term(3),
          ),
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
        ),
      ),
    )
    checkSerialization(
      CommitLogStoreActor.AppendCommittedEntriesResponse(LogEntryIndex(4)),
    )

    // raft.protocol
    checkSerialization(
      RequestVote(
        NormalizedShardId.from("shard:need?url&encode"),
        Term(293081),
        MemberIndex("role?need&url:encode"),
        LogEntryIndex(1380),
        Term(11284196),
      ),
    )
    checkSerialization(
      RequestVoteAccepted(
        Term(21381),
        MemberIndex("role?member:abc"),
      ),
    )
    checkSerialization(
      RequestVoteDenied(
        Term(24714),
      ),
    )
    checkSerialization(
      AppendEntries(
        NormalizedShardId.from("shard-id/12831"),
        Term(18231),
        MemberIndex("role/19021"),
        LogEntryIndex(1201),
        Term(18230),
        Seq(
          LogEntry(LogEntryIndex(2173491), EntityEvent(None, MyEvent(21, "my-event:abc:128")), Term(121)),
          LogEntry(
            LogEntryIndex(2718941),
            EntityEvent(Some(NormalizedEntityId.from("entity/12319?abc")), MyEvent(22, "my-event:abc:128")),
            Term(121),
          ),
        ),
        LogEntryIndex(3741890),
      ),
    )
    checkSerialization(
      AppendEntriesSucceeded(
        Term(123),
        LogEntryIndex(83510),
        MemberIndex("role/2914?"),
      ),
    )
    checkSerialization(
      AppendEntriesFailed(
        Term(2149015),
        MemberIndex("role(new)"),
      ),
    )
    checkSerialization(
      InstallSnapshot(
        NormalizedShardId.from("shard/1238?a"),
        Term(21491),
        MemberIndex("role/1231"),
        Term(2184015),
        LogEntryIndex(15715),
      ),
    )
    checkSerialization(
      InstallSnapshotSucceeded(
        NormalizedShardId.from("shard/1231"),
        Term(12301),
        LogEntryIndex(214757),
        MemberIndex("role/me"),
      ),
    )
    checkSerialization(
      SuspendEntity(
        NormalizedShardId.from("shard/somewhere"),
        NormalizedEntityId.from("entity/someone"),
        MyStopMessage,
      ),
    )
    checkSerialization(
      TryCreateEntity(
        NormalizedShardId.from("shard/somewhere"),
        NormalizedEntityId.from("entity/someone"),
      ),
    )

    // raft.snapshot
    checkSerialization(
      EntitySnapshot(
        EntitySnapshotMetadata(NormalizedEntityId.from("entity/jp:user:1275"), LogEntryIndex(74165)),
        EntityState(MyEntity(28656, "me", 80)),
      ),
    )

    //raft.snapshot.sync
    checkSerialization(SyncCompleted(NoOffset))
    checkSerialization(SyncCompleted(Sequence(124)))
    checkSerialization(SyncCompleted(TimeBasedUUID(UUID.fromString("ed286108-8a13-11eb-8dcd-0242ac130003"))))
    checkSerialization(SyncProgress(NoOffset))
    checkSerialization(SyncProgress(Sequence(283)))
    checkSerialization(SyncProgress(TimeBasedUUID(UUID.fromString("ed286108-8a13-11eb-8dcd-0242ac130003"))))
    Seq(NoOffset, Sequence(345), TimeBasedUUID(UUID.fromString("04b3ee72-8d39-11ec-9503-00155da8e61b"))).foreach {
      offset =>
        checkSerialization(
          SnapshotCopied(
            offset,
            MemberIndex("member&need%url?encode"),
            NormalizedShardId.from("shard&need%url?encode"),
            Term(85714),
            LogEntryIndex(2357),
            Set(
              NormalizedEntityId.from("shard1"),
              NormalizedEntityId.from("shard2&need%url?encode"),
              NormalizedEntityId.from("shard3"),
            ),
          ),
        )
    }

    // raft.model
    checkSerialization(NoOp)

    // typed
    checkSerialization(ReplicationEnvelope("entity/jp:user:1275", NoOp))
  }

  "manifest" when {
    "a given object is not an instance of ClusterReplicationSerializable" should {
      "throw an IllegalArgumentException" in {
        case class InvalidObject()
        a[IllegalArgumentException] shouldBe thrownBy {
          serializer.manifest(InvalidObject())
        }
      }
    }
  }

  "toBinary" when {
    "a given object is not an instance of ClusterReplicationSerializable" should {
      "throw an IllegalArgumentException" in {
        case class InvalidObject()
        a[IllegalArgumentException] shouldBe thrownBy {
          serializer.toBinary(InvalidObject())
        }
      }
    }
  }

  "fromBinary" when {
    "a given manifest is invalid" should {
      "throw a NotSerializableException" in {
        val invalidManifest = "THIS_IS_INVALID_MANIFEST"
        a[NotSerializableException] shouldBe thrownBy {
          serializer.fromBinary(Array.empty, invalidManifest)
        }
      }
    }
  }

}
