package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import akka.actor.typed.scaladsl.adapter._
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftActor.Follower
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, NoOp, ReplicatedLog, Term }
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._

object RaftActorSpec {
  final case object DummyEntityState
}

class RaftActorSpec extends TestKit(ActorSystem()) with RaftActorSpecBase {
  import RaftActorSpec._

  "RaftActor receiving FetchEntityEvents" should {
    "reply FetchEntityEventsResponse" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )

      val replyProbe = TestProbe()
      val term       = Term.initial().next()
      val entityId   = NormalizedEntityId.from("test-entity")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), term),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "a"), term),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "b"), term),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "c"), term),
      )
      val data = RaftMemberData(
        replicatedLog = ReplicatedLog().merge(logEntries, LogEntryIndex.initial()),
        lastApplied = LogEntryIndex(3),
      )
      setState(follower, Follower, data)
      assume(data.lastApplied <= data.replicatedLog.lastLogIndex)

      follower ! FetchEntityEvents(
        entityId,
        from = LogEntryIndex.initial(),
        to = data.lastApplied,
        replyTo = replyProbe.ref,
      )
      val reply = replyProbe.expectMsgType[FetchEntityEventsResponse]
      reply.events.map(_.index) should be(Seq(LogEntryIndex(2), LogEntryIndex(3)))
    }
  }

  "RaftActor Snapshotting" should {

    val raftConfig = ConfigFactory
      .parseString("""
                     | lerna.akka.entityreplication.raft {
                     |   election-timeout = 99999s
                     |   # ログの長さが 3 を超えている場合は snapshot をとる
                     |   compaction.log-size-threshold = 3
                     |   compaction.preserve-log-size = 1
                     |   compaction.log-size-check-interval = 1s
                     | }
                     |""".stripMargin).withFallback(ConfigFactory.load())

    "ログが追加された後にログの長さがしきい値を超えている場合はスナップショットがとられる" in {
      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
      )

      val leaderMemberIndex = createUniqueMemberIndex()
      val term              = Term.initial().next()
      val entityId          = NormalizedEntityId.from("test-entity")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), term),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), term),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), term),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "d"), term),
      )
      val applicableIndex = LogEntryIndex(3)
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = applicableIndex,
      )

      val command =
        replicationActor.fishForSpecificMessage() {
          case msg: TakeSnapshot => msg
        }
      command.metadata shouldBe EntitySnapshotMetadata(entityId, applicableIndex)
    }

    "全スナップショットの永続化が完了すると Entity に適用済みの部分までログが切り詰められる" in {

      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
      )

      val leaderMemberIndex = createUniqueMemberIndex()
      val term              = Term.initial().next()
      val entityId1         = NormalizedEntityId.from("test-entity-1")
      val entityId2         = NormalizedEntityId.from("test-entity-2")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId1), "a"), term),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "b"), term),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId2), "c"), term),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId2), "d"), term),
      )
      val applicableIndex = LogEntryIndex(3)
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = applicableIndex,
      )
      // entityId1 と entityId2 の両方に TakeSnapshot が配信されるので、それぞれ reply
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      snapshotStore.receiveWhile(messages = 2) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
          msg
      }
      awaitAssert {
        import org.scalatest.LoneElement._
        getState(follower).stateData.replicatedLog.entries should have size 1
        getState(follower).stateData.replicatedLog.entries.loneElement shouldBe logEntries.last
      }
    }

    "1つでもスナップショットの取得に失敗した Entity が居た場合はログが切り詰められない" in {

      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
      )

      val leaderMemberIndex = createUniqueMemberIndex()
      val term              = Term.initial().next()
      val entityId1         = NormalizedEntityId.from("test-entity-1")
      val entityId2         = NormalizedEntityId.from("test-entity-2")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId1), "a"), term),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "b"), term),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId2), "c"), term),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId2), "d"), term),
      )
      val applicableIndex = LogEntryIndex(3)
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = applicableIndex,
      )
      // entityId1 と entityId2 の両方に TakeSnapshot が配信されるので、それぞれ reply
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      replicationActor.fishForSpecificMessage() {
        case _: TakeSnapshot =>
        // do not reply Snapshot
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
          msg
      }
      snapshotStore.expectNoMessage()

      awaitAssert(getState(follower).stateData.replicatedLog.entries should have size logEntries.size)
    }

    "1つでもスナップショットの永続化に失敗した Entity が居た場合はログが切り詰められない" in {

      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
      )

      val leaderMemberIndex = createUniqueMemberIndex()
      val term              = Term.initial().next()
      val entityId1         = NormalizedEntityId.from("test-entity-1")
      val entityId2         = NormalizedEntityId.from("test-entity-2")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId1), "a"), term),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "b"), term),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId2), "c"), term),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId2), "d"), term),
      )
      val applicableIndex = LogEntryIndex(3)
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = applicableIndex,
      )
      // entityId1 と entityId2 の両方に TakeSnapshot が配信されるので、それぞれ reply
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotFailure(msg.snapshot.metadata))
      }

      awaitAssert(getState(follower).stateData.replicatedLog.entries should have size logEntries.size)
    }

    "ログの取得や永続化に一度失敗したとしても、再度スナップショットの取得が行われる" in {

      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
      )

      val leaderMemberIndex = createUniqueMemberIndex()
      val term              = Term.initial().next()
      val entityId1         = NormalizedEntityId.from("test-entity-1")
      val entityId2         = NormalizedEntityId.from("test-entity-2")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId1), "a"), term),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "b"), term),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId2), "c"), term),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId2), "d"), term),
      )
      val applicableIndex = LogEntryIndex(3)
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = applicableIndex,
      )
      // 1 回目は永続化に失敗
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotFailure(msg.snapshot.metadata))
      }
      // 2 回目は成功
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      snapshotStore.receiveWhile(messages = 2) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      }

      awaitAssert {
        import org.scalatest.LoneElement._
        getState(follower).stateData.replicatedLog.entries should have size 1
        getState(follower).stateData.replicatedLog.entries.loneElement shouldBe logEntries.last
      }
    }

    "not persist snapshots that have already been persisted in the next compaction" in {
      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
      )

      val leaderMemberIndex = createUniqueMemberIndex()
      val term              = Term.initial().next()
      val entityId1         = NormalizedEntityId.from("test-entity-1")
      val entityId2         = NormalizedEntityId.from("test-entity-2")

      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId1), "a"), term),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId1), "b"), term),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId1), "c"), term),
          LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId1), "d"), term),
        ),
        leaderCommit = LogEntryIndex(4),
      )
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          msg.metadata.entityId should be(entityId1)
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      }

      // add events that only entity2 persisted
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(4),
        prevLogTerm = term,
        entries = Seq(
          LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId2), "e"), term),
          LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId2), "f"), term),
          LogEntry(LogEntryIndex(7), EntityEvent(Option(entityId2), "g"), term),
        ),
        leaderCommit = LogEntryIndex(7),
      )
      // the snapshot should be only for entity2
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          msg.metadata.entityId should be(entityId2)
          replicationActor.reply(Snapshot(msg.metadata, EntityState(DummyEntityState)))
      }
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          msg.snapshot.metadata.entityId should be(entityId2)
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      }
    }
  }
}
