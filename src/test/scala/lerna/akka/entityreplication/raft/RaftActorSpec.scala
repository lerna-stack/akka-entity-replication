package lerna.akka.entityreplication.raft

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ typed, Actor, ActorSystem, Props }
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId }
import lerna.akka.entityreplication.raft.RaftActor.{ AppendedEntries_V2_1_0, Follower }
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands.{ AppendEntriesSucceeded, InstallSnapshot }
import lerna.akka.entityreplication.raft.protocol.{ FetchEntityEvents, FetchEntityEventsResponse }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import org.scalatest.Inside

import scala.annotation.nowarn

object RaftActorSpec {
  final case object DummyEntityState
}

class RaftActorSpec
    extends TestKit(ActorSystem("RaftActorSpec", RaftActorSpecBase.configWithPersistenceTestKits))
    with RaftActorSpecBase
    with Inside {
  import RaftActorSpec._

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
  private val persistenceTestKit                               = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
  }

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
        replicatedLog = ReplicatedLog().truncateAndAppend(logEntries),
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

    "全スナップショットの永続化が完了すると、eventSourcingIndex (CommitLogStore に保存できたLogEntry の Index) までログが切り詰められる" in {

      val snapshotStore       = TestProbe()
      val replicationActor    = TestProbe()
      val commitLogStore      = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        commitLogStore = commitLogStore.ref,
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
      follower ! createAppendEntries(
        shardId,
        term,
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = LogEntryIndex(4),
      )

      // To advance compaction target entries, CommitLogStore should handle AppendCommittedEntries.
      val eventSourcingIndex = LogEntryIndex(3)
      commitLogStore.expectMsg(CommitLogStoreActor.AppendCommittedEntries(shardId, Seq.empty))
      commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(eventSourcingIndex))

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
      val commitLogStore      = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
        commitLogStore = commitLogStore.ref,
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

      // To advance compaction target entries, CommitLogStore should handle AppendCommittedEntries.
      commitLogStore.expectMsg(CommitLogStoreActor.AppendCommittedEntries(shardId, Seq.empty))
      commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(applicableIndex))

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
      val commitLogStore      = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
        commitLogStore = commitLogStore.ref,
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

      // To advance compaction target entries, CommitLogStore should handle AppendCommittedEntries.
      commitLogStore.expectMsg(CommitLogStoreActor.AppendCommittedEntries(shardId, Seq.empty))
      commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(applicableIndex))

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
      val commitLogStore      = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        settings = RaftSettings(raftConfig),
        commitLogStore = commitLogStore.ref,
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

      // To advance compaction target entries, CommitLogStore should handle AppendCommittedEntries.
      commitLogStore.expectMsg(CommitLogStoreActor.AppendCommittedEntries(shardId, Seq.empty))
      commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(applicableIndex))

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

    "prevent to start snapshot synchronization during compaction" in {
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
      val entityId          = NormalizedEntityId.from("test-entity")
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "d"), Term(1)),
      )
      val installSnapshot =
        InstallSnapshot(
          shardId,
          Term(3),
          leaderMemberIndex,
          srcLatestSnapshotLastLogTerm = Term(2),
          srcLatestSnapshotLastLogLogIndex = LogEntryIndex(5),
        )
      follower ! createAppendEntries(
        shardId,
        Term(1),
        leaderMemberIndex,
        entries = logEntries,
        leaderCommit = LogEntryIndex(3),
      )

      // wait for starting compaction
      val takeSnapshot =
        replicationActor.fishForSpecificMessage() {
          case msg: TakeSnapshot => msg
        }
      LoggingTestKit.info("Skipping snapshot synchronization because compaction is in progress").expect {
        follower ! installSnapshot
      }
      takeSnapshot.replyTo ! Snapshot(takeSnapshot.metadata, EntityState(DummyEntityState))
      snapshotStore.receiveWhile(messages = 1) {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      } should have length 1
      // compaction completed (snapshot synchronization become available)
      LoggingTestKit.info("Snapshot synchronization started").expect {
        follower ! installSnapshot
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

    "warn and continue if the compaction might not delete enough log entries" in {
      val commitLogStore      = TestProbe()
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val raftSettings        = RaftSettings(raftConfig)
      val replicationActor    = TestProbe()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        commitLogStore = commitLogStore.ref,
        settings = raftSettings,
        replicationActor = replicationActor.ref,
      )

      // The compaction cannot delete any entries by setting eventSourcingIndex to 0
      val eventSourcingIndex = LogEntryIndex(0)
      commitLogStore.expectMsg(CommitLogStoreActor.AppendCommittedEntries(shardId, Seq.empty))
      commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(eventSourcingIndex))

      assume(raftSettings.compactionLogSizeThreshold == 3)
      LoggingTestKit
        .warn(
          "[Follower] Compaction might not delete enough entries, but will continue to reduce log size as possible " +
          "(even if this compaction continues, the remaining entries might trigger new compaction at the next tick). " +
          "Estimated compacted log size is [3] entries (lastApplied [3], eventSourcingIndex [Some(0)], preserveLogSize [1]), " +
          "however compaction.log-size-threshold is [3] entries. " +
          "This warning might happen if event sourcing is too slow or compaction is too fast (or too slow). " +
          "If this warning continues, please consult settings related to event sourcing and compaction.",
        ).expect {
          val leaderMemberIndex = createUniqueMemberIndex()
          val entityId          = NormalizedEntityId.from("entity1")
          val logEntries = Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          )
          follower ! createAppendEntries(
            shardId,
            Term(1),
            leaderMemberIndex,
            entries = logEntries,
            leaderCommit = LogEntryIndex(3),
          )

          val takeSnapshot = replicationActor.fishForSpecificMessage() {
            case msg: TakeSnapshot => msg
          }
          takeSnapshot.metadata shouldBe EntitySnapshotMetadata(entityId, LogEntryIndex(3))
        }
    }

  }

  "RaftActor" should {

    "recover by reading AppendedEntries_V2_1_0 for backward compatibility" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()

      val followerPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = followerMemberIndex)
      persistenceTestKit.persistForRecovery(
        followerPersistenceId,
        Seq(
          AppendedEntries_V2_1_0(
            Term(1),
            Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(1)),
            ),
            LogEntryIndex(0),
          ): @nowarn,
          AppendedEntries_V2_1_0(
            Term(1),
            Seq(
              LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId.from("1")), "event-2"), Term(1)),
            ),
            LogEntryIndex(2),
          ): @nowarn,
          AppendedEntries_V2_1_0(
            Term(2),
            Seq(
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(2)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(NormalizedEntityId.from("2")), "event-2"), Term(2)),
            ),
            LogEntryIndex(2),
          ): @nowarn,
          AppendedEntries_V2_1_0(
            Term(2),
            Seq(
              LogEntry(LogEntryIndex(4), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(2)),
            ),
            LogEntryIndex(3),
          ): @nowarn,
        ),
      )
      val expectedEntriesAfterRecover = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId.from("1")), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        LogEntry(LogEntryIndex(4), EntityEvent(Some(NormalizedEntityId.from("2")), "event-1"), Term(2)),
      )

      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val recoveredLogEntries = getState(follower).stateData.replicatedLog.entries
      recoveredLogEntries should contain theSameElementsInOrderAs expectedEntriesAfterRecover
      recoveredLogEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntriesAfterRecover.map(_.event)
    }

    "send ReplicationFailed messages to conflict clients and discard the clients " +
    "if the received AppendEntries message contains conflict entries" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
      )
      val clientProbe1 = TestProbe()
      val clientProbe2 = TestProbe()
      val client1      = ClientContext(clientProbe1.ref, Some(EntityInstanceId(1)), Some(TestProbe().ref))
      val client2      = ClientContext(clientProbe2.ref, Some(EntityInstanceId(2)), None)
      val followerData = {
        val replicatedLog = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Some(NormalizedEntityId("entity-1")), "event-a"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Some(NormalizedEntityId("entity-2")), "event-b"), Term(1)),
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(2)),
          ),
        )
        RaftMemberData(currentTerm = Term(2), replicatedLog = replicatedLog, commitIndex = LogEntryIndex(1))
          .registerClient(client1, LogEntryIndex(2))
          .registerClient(client2, LogEntryIndex(3))
      }
      setState(follower, Follower, followerData)

      val leaderMemberIndex = createUniqueMemberIndex()
      val appendEntries = createAppendEntries(
        shardId,
        Term(3),
        leaderMemberIndex,
        prevLogIndex = LogEntryIndex(1),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(3)),
        ),
      )
      val regionProbe = TestProbe()
      follower.tell(appendEntries, regionProbe.ref)
      regionProbe.expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(2), followerMemberIndex))

      // Verifies that the RaftActor discards conflict clients.
      inside(getState(follower).stateData) { newFollowerData =>
        assert(!newFollowerData.clients.contains(LogEntryIndex(2)))
        assert(!newFollowerData.clients.contains(LogEntryIndex(3)))
      }

      // Verifies that each conflict client receives a ReplicationFailed message
      clientProbe1.expectMsg(ReplicationFailed)
      assert(clientProbe1.sender() == client1.originSender.get) // including the original sender
      clientProbe2.expectMsg(ReplicationFailed)
      assert(clientProbe2.sender() == system.deadLetters) // including no original sender

    }

    "stop itself when its shard id is defined as disabled" in {
      val shardId = createUniqueShardId()
      val raftConfig = ConfigFactory
        .parseString(s"""
            | lerna.akka.entityreplication.raft.disabled-shards = ["${shardId.raw}"]
            |""".stripMargin).withFallback(ConfigFactory.load())
      val ref = system.actorOf(
        Props(
          new RaftActor(
            typeName = defaultTypeName,
            extractEntityId = {
              case msg => (NormalizedEntityId.from("dummy"), msg)
            },
            replicationActorProps = _ =>
              Props(new Actor() {
                override def receive: Receive = {
                  case message => system.deadLetters forward message
                }
              }),
            _region = TestProbe().ref,
            shardSnapshotStoreProps = Props(new Actor {
              override def receive: Receive = {
                case msg => TestProbe().ref forward msg
              }
            }),
            _selfMemberIndex = createUniqueMemberIndex(),
            _otherMemberIndexes = Set(),
            settings = RaftSettings(raftConfig),
            _commitLogStore = TestProbe().ref,
          ),
        ),
        name = shardId.underlying,
      )
      watch(ref)
      expectTerminated(ref)
    }

    "notice warning log when it is defined as sticky leader" in {
      val shardId             = createUniqueShardId()
      val followerMemberIndex = createUniqueMemberIndex()
      val customSettings =
        RaftSettings(defaultRaftConfig).withStickyLeaders(Map(shardId.raw -> followerMemberIndex.role))
      LoggingTestKit.warn("This raft actor is defined as sticky leader").expect {
        createRaftActor(
          shardId = shardId,
          selfMemberIndex = followerMemberIndex,
          settings = customSettings,
        )
      }
    }
  }

}
