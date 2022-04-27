package lerna.akka.entityreplication.raft

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.internal.ClusterReplicationSettingsImpl
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftProtocol.{
  Command,
  ProcessCommand,
  Replica,
  Replicate,
  ReplicationFailed,
  ReplicationSucceeded,
}
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.testkit.CustomTestProbe._
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.{
  EntitySnapshot,
  EntitySnapshotMetadata,
  EntityState,
}
import lerna.akka.entityreplication.raft.snapshot.{ ShardSnapshotStore, SnapshotProtocol }
import lerna.akka.entityreplication.util.EventStore
import org.scalatest.Inside

import scala.concurrent.duration.DurationInt

class RaftActorLeaderSpec
    extends TestKit(ActorSystem("RaftActorLeaderSpec", RaftActorSpecBase.configWithPersistenceTestKits))
    with RaftActorSpecBase
    with Inside {

  import RaftActor._
  private implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

  private[this] val entityId = NormalizedEntityId.from("test-entity")
  private[this] val shardId  = NormalizedShardId.from("test-shard")

  private val persistenceTestKit = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
  }

  "Leader" should {

    "AppendEntries が古い Term を持っているときは拒否" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term1              = Term.initial()
      val term2              = term1.next()
      setState(leader, Leader, createLeaderData(term2))

      leader ! createAppendEntries(shardId, term1, anotherMemberIndex)
      expectMsg(AppendEntriesFailed(term2, leaderMemberIndex))
    }

    "AppendEntries が新しい Term を持っているときは Follower に降格" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val anotherMemberIndex = createUniqueMemberIndex()
      val term1              = Term.initial()
      val term2              = term1.next()
      setState(leader, Leader, createLeaderData(term1))

      leader ! createAppendEntries(shardId, term2, anotherMemberIndex)
      expectMsg(AppendEntriesSucceeded(term2, LogEntryIndex(0), leaderMemberIndex))

      val state = getState(leader)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(term2)
      state.stateData.leaderMember should contain(anotherMemberIndex)
    }

    "コマンドを ReplicationActor に転送する" ignore {}

    "AppendEntries が新しい Term を持っていて、prevLogIndex/prevLogTerm に一致するログエントリがある場合は AppendEntriesSucceeded" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
      )
      val log = ReplicatedLog().truncateAndAppend(logEntries)
      setState(leader, Leader, createLeaderData(Term(1), log))

      leader ! createAppendEntries(
        shardId,
        Term(2),
        newLeaderMemberIndex,
        LogEntryIndex(2),
        Term(1),
        Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        ),
      )
      expectMsg(AppendEntriesSucceeded(Term(2), LogEntryIndex(3), leaderMemberIndex))
    }

    "AppendEntries が新しい Term を持っていて、prevLogIndex/prevLogTerm に一致するログエントリがない場合は AppendEntriesFailed" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().truncateAndAppend(logEntries)
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index3, term1, logEntries)
      expectMsg(AppendEntriesFailed(term2, leaderMemberIndex))

      val state = getState(leader)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(term2)
      state.stateData.leaderMember should contain(newLeaderMemberIndex)
    }

    "prevLogIndex の Term が prevLogTerm に一致するログエントリでない場合は AppendEntriesFailed を返す" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val index4               = LogEntryIndex(4)
      val followerLogEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
        LogEntry(index3, EntityEvent(Option(entityId), "c"), term1),
      )
      val leaderLogEntries = Seq(
        LogEntry(index4, EntityEvent(Option(entityId), "e"), term1.next()),
      )
      val log = ReplicatedLog().truncateAndAppend(followerLogEntries)
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term1, newLeaderMemberIndex, index3, term1.next(), leaderLogEntries)
      expectMsg(AppendEntriesFailed(term1, leaderMemberIndex))
    }

    "leaderCommit > commitIndex となる場合、 commitIndex に min(leaderCommit, 新規エントリの最後のインデックス) を設定" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      // leaderCommit > commitIndex
      val index2       = LogEntryIndex(2)
      val leaderCommit = LogEntryIndex(3)
      val logEntries1 = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), term1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), term1),
      )
      val log        = ReplicatedLog().truncateAndAppend(logEntries1)
      val leaderData = createLeaderData(term1, log, index2)

      val logEntries2 = Seq(
        LogEntry(leaderCommit, EntityEvent(Option(entityId), "c"), term1),
      )
      setState(leader, Leader, leaderData)
      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index2, term1, logEntries2, leaderCommit)
      expectMsg(AppendEntriesSucceeded(term2, leaderCommit, leaderMemberIndex))

      getState(leader).stateData.commitIndex should be(leaderCommit)
    }

    "keep commitIndex even if leaderCommit is less than commitIndex" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val term1LogEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log        = ReplicatedLog().truncateAndAppend(term1LogEntries)
      val leaderData = createLeaderData(term1, log, commitIndex = index2)
      setState(leader, Leader, leaderData)
      // index2 already committed but new Leader doesn't know that

      // new Leader elected
      val term2LogEntries = Seq(
        LogEntry(index3, EntityEvent(None, NoOp), term2),
      )
      leader ! createAppendEntries(
        shardId,
        term = term2,
        newLeaderMemberIndex,
        prevLogIndex = index2,
        prevLogTerm = term1,
        term2LogEntries,
        leaderCommit = index1,
      )
      expectMsg(AppendEntriesSucceeded(term2, index3, leaderMemberIndex))

      getState(leader).stateData.commitIndex should be(index2)
    }

    "persist the whole new entries starting with the lastLogIndex + 1 " +
    "if the received AppendEntries message contains no existing entries" in {
      val shardId           = createUniqueShardId()
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        shardId = shardId,
        selfMemberIndex = leaderMemberIndex,
      )
      val leaderPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = leaderMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(leader, Leader, createLeaderData(Term(2), replicatedLog, commitIndex = LogEntryIndex(3)))

      val newLeaderMemberIndex = createUniqueMemberIndex()
      leader ! createAppendEntries(
        shardId,
        Term(3),
        newLeaderMemberIndex,
        prevLogIndex = LogEntryIndex(3),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(5), leaderMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](leaderPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(leader)) { leaderState =>
        leaderState.stateName should be(Follower)
        inside(leaderState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "persist only new entries starting with the lastLogIndex + 1 " +
    "if the received AppendEntries message contains some existing entries" in {
      val shardId           = createUniqueShardId()
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        shardId = shardId,
        selfMemberIndex = leaderMemberIndex,
      )
      val leaderPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = leaderMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event-1"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(leader, Leader, createLeaderData(Term(2), replicatedLog, commitIndex = LogEntryIndex(2)))

      val newLeaderMemberIndex = createUniqueMemberIndex()
      leader ! createAppendEntries(
        shardId,
        Term(3),
        newLeaderMemberIndex,
        prevLogIndex = LogEntryIndex(2),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event-1"), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(5), leaderMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](leaderPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(leader)) { leaderState =>
        leaderState.stateName should be(Follower)
        inside(leaderState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event-1"), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(3)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "persist only new entries (beginning with the first conflict) " +
    "if the received AppendEntries message contains conflict entries" in {
      val shardId           = createUniqueShardId()
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        shardId = shardId,
        selfMemberIndex = leaderMemberIndex,
      )
      val leaderPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = leaderMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(4), EntityEvent(Some(entityId), "event-2"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(leader, Leader, createLeaderData(Term(2), replicatedLog, commitIndex = LogEntryIndex(3)))

      val newLeaderMemberIndex = createUniqueMemberIndex()
      leader ! createAppendEntries(
        shardId,
        Term(4),
        newLeaderMemberIndex,
        prevLogIndex = LogEntryIndex(3),
        prevLogTerm = Term(2),
        entries = Seq(
          LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
          LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(4)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(4), LogEntryIndex(5), leaderMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](leaderPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(4))
          val expectedEntries = Seq(
            LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
            LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(4)),
          )
          appendedEntries.logEntries should contain theSameElementsInOrderAs expectedEntries
          appendedEntries.logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
      }
      inside(getState(leader)) { leaderState =>
        leaderState.stateName should be(Follower)
        inside(leaderState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event-1"), Term(1)),
              LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(4)),
              LogEntry(LogEntryIndex(5), EntityEvent(Some(entityId), "event-2"), Term(4)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "truncate no entries even if the received AppendEntries message contains all existing entries (not including the last entry)" in {
      val shardId           = createUniqueShardId()
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        shardId = shardId,
        selfMemberIndex = leaderMemberIndex,
      )
      val leaderPersistenceId = raftActorPersistenceId(shardId = shardId, selfMemberIndex = leaderMemberIndex)
      val replicatedLog = {
        val logEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event-1"), Term(2)),
        )
        ReplicatedLog().truncateAndAppend(logEntries)
      }
      setState(leader, Leader, createLeaderData(Term(2), replicatedLog, commitIndex = LogEntryIndex(2)))

      val newLeaderMemberIndex = createUniqueMemberIndex()
      leader ! createAppendEntries(
        shardId,
        Term(3),
        newLeaderMemberIndex,
        prevLogIndex = LogEntryIndex(1),
        prevLogTerm = Term(1),
        entries = Seq(
          LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          // The following entries will be sent in another AppendEntries batch.
          // LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event-1"), Term(2)),
          // LogEntry(LogEntryIndex(4), EntityEvent(None, NoOp), Term(3)),
        ),
      )

      expectMsg(AppendEntriesSucceeded(Term(3), LogEntryIndex(3), leaderMemberIndex))
      inside(persistenceTestKit.expectNextPersistedType[AppendedEntries](leaderPersistenceId)) {
        case appendedEntries =>
          appendedEntries.term should be(Term(3))
          appendedEntries.logEntries should be(empty)
      }
      inside(getState(leader)) { leaderState =>
        leaderState.stateName should be(Follower)
        inside(leaderState.stateData.replicatedLog.entries) {
          case logEntries =>
            val expectedEntries = Seq(
              LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
              LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
              LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event-1"), Term(2)),
            )
            logEntries should contain theSameElementsInOrderAs expectedEntries
            logEntries.map(_.event) should contain theSameElementsInOrderAs expectedEntries.map(_.event)
        }
      }
    }

    "handle AppendEntriesSucceeded with the current term, " +
    "update matchIndex and nextIndex for the follower(sender), " +
    "and do nothing if the index update doesn't contribute a new commit" in {
      // If the AppendEntriesSucceeded contributes new commits, the commit index will change. Furthermore, the leader will
      // send ReplicationSucceeded to ReplicationActor. This test verifies such changes won't happen.
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val replicationActor    = TestProbe()
      val clientActor         = TestProbe()
      val settings = RaftSettings(
        ConfigFactory
          .parseString(
            """
              |lerna.akka.entityreplication.raft.multi-raft-roles = [
              |  "replica-group-1", "replica-group-2", "replica-group-3", "replica-group-4"
              |]
              |""".stripMargin,
          ).withFallback(defaultRaftConfig),
      )
      assert(settings.replicationFactor > 3, "For making no new commit, replicationFactor should be greater than 3.")
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          ),
        )
        val clientContext = ClientContext(
          replicationActor.ref,
          Some(EntityInstanceId(1)),
          Some(clientActor.ref),
        )
        createLeaderData(
          currentTerm = Term(1),
          log = log,
          commitIndex = LogEntryIndex(1),
          lastApplied = LogEntryIndex(1),
        )
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(1))
          .registerClient(clientContext, LogEntryIndex(2))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
        settings = settings,
        replicationActor = replicationActor.ref,
        entityId = entityId,
      )
      setState(leader, Leader, leaderData)

      val oldStateData = getState(leader).stateData
      oldStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))
      oldStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(1))
      oldStateData.commitIndex should be(LogEntryIndex(1))
      oldStateData.lastApplied should be(LogEntryIndex(1))

      leader ! AppendEntriesSucceeded(Term(1), LogEntryIndex(2), followerMemberIndex)

      val newStateData = getState(leader).stateData
      newStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(3))
      newStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(2))
      newStateData.commitIndex should be(LogEntryIndex(1))
      newStateData.lastApplied should be(LogEntryIndex(1))

      replicationActor.expectNoMessage()
      clientActor.expectNoMessage()
    }

    "handle AppendEntriesSucceeded with the current term " +
    "and send ReplicationSucceeded with new committed entry (== NoOp) to the client (the leader itself)" in {
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val replicationActor    = TestProbe()

      // The leader will send ReplicationSucceeded to ClientContext.replyTo (this should be the leader itself).
      val clientContextReplyTo = TestProbe()
      val clientContext        = ClientContext(clientContextReplyTo.ref, None, None)

      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
        createLeaderData(
          currentTerm = Term(2),
          log = log,
          commitIndex = LogEntryIndex(1),
          lastApplied = LogEntryIndex(1),
        )
          .registerClient(clientContext, LogEntryIndex(2))
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(1))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
        replicationActor = replicationActor.ref,
        entityId = entityId,
      )
      setState(leader, Leader, leaderData)

      val oldStateData = getState(leader).stateData
      oldStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))
      oldStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(1))
      oldStateData.commitIndex should be(LogEntryIndex(1))
      oldStateData.lastApplied should be(LogEntryIndex(1))

      leader ! AppendEntriesSucceeded(Term(2), LogEntryIndex(2), followerMemberIndex)

      val newStateData = getState(leader).stateData
      newStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(3))
      newStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(2))
      newStateData.commitIndex should be(LogEntryIndex(2))
      newStateData.lastApplied should be(LogEntryIndex(2))

      clientContextReplyTo.expectMsg(
        ReplicationSucceeded(NoOp, LogEntryIndex(2), None),
      )
      replicationActor.expectNoMessage()
    }

    "handle AppendEntriesSucceeded with the current term " +
    "and send ReplicationSucceeded with new committed entry (!= NoOp) to the registered client" in {
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val replicationActor    = TestProbe()
      val clientActor         = TestProbe()
      val clientContext = ClientContext(
        replicationActor.ref,
        Some(EntityInstanceId(1)),
        Some(clientActor.ref),
      )
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          ),
        )
        createLeaderData(
          currentTerm = Term(1),
          log = log,
          commitIndex = LogEntryIndex(1),
          lastApplied = LogEntryIndex(1),
        )
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(1))
          .registerClient(clientContext, LogEntryIndex(2))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
        replicationActor = replicationActor.ref,
        entityId = entityId,
      )
      setState(leader, Leader, leaderData)

      val oldStateData = getState(leader).stateData
      oldStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))
      oldStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(1))
      oldStateData.commitIndex should be(LogEntryIndex(1))
      oldStateData.lastApplied should be(LogEntryIndex(1))

      leader ! AppendEntriesSucceeded(Term(1), LogEntryIndex(2), followerMemberIndex)

      val newStateData = getState(leader).stateData
      newStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(3))
      newStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(2))
      newStateData.commitIndex should be(LogEntryIndex(2))
      newStateData.lastApplied should be(LogEntryIndex(2))

      replicationActor.expectMsg(
        ReplicationSucceeded("event1", LogEntryIndex(2), clientContext.instanceId),
      )
      replicationActor.lastSender should be(clientActor.ref)
      // The leader should not send any message to the client.
      // ReplicationActor will send a response message after applying the new committed event to its state.
      clientActor.expectNoMessage()
    }

    "handle AppendEntriesSucceeded with the current term, " +
    "send Replica with new committed event (!= NoOp) to its ReplicationActor if a client is unknown, " +
    "unstash stashed Commands, and then send unstashed Commands to ReplicationActor" in {
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val replicationActor    = TestProbe()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(None, NoOp), Term(2)),
          ),
        )
        createLeaderData(currentTerm = Term(2), log = log)
          // This follower doesn't have any entries yet for some reason, like network partition.
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(0))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
        replicationActor = replicationActor.ref,
        entityId = entityId,
      )
      setState(leader, Leader, leaderData)

      val oldStateData = getState(leader).stateData
      oldStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(1))
      oldStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(0))
      oldStateData.commitIndex should be(LogEntryIndex(0))
      oldStateData.lastApplied should be(LogEntryIndex(0))
      oldStateData.clients should not contain key(LogEntryIndex(1))

      // The leader is elected, but all entries is not committed yet (e.g. full cluster restart).
      // The leader can receives Commands from
      //   * Another follower that knows the leader (where replicationFactor > 3)
      //   * ReplicationRegion
      leader ! Command("stashed command 1")
      leader ! Command("stashed command 2")

      leader ! AppendEntriesSucceeded(Term(2), LogEntryIndex(3), followerMemberIndex)

      val newStateData = getState(leader).stateData
      newStateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(4))
      newStateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(3))
      newStateData.commitIndex should be(LogEntryIndex(3))
      newStateData.lastApplied should be(LogEntryIndex(3))

      val replica = replicationActor.fishForSpecificMessage[Replica]() {
        case replica: Replica => replica
      }
      replica.logEntry.index should be(LogEntryIndex(2))
      replica.logEntry.term should be(Term(1))
      replica.logEntry.event.entityId should contain(entityId)
      replica.logEntry.event.event should be("event1")

      replicationActor.expectMsg(ProcessCommand("stashed command 1"))
      replicationActor.expectMsg(ProcessCommand("stashed command 2"))
    }

    "ignore AppendEntriesSucceeded with a newer term and warn" in {
      // If the leader handles AppendEntriesSucceeded, the next index and the match index for the follower(sender) will
      // change. This test verifies that AppendEntriesSucceeded with a newer term won't trigger such changes. In another
      // aspect, if the leader handles AppendEntriesFailed with a newer term, the leader will update its term and become
      // a follower. This test also verifies AppendEntriesSucceeded with a newer term won't trigger such changes.
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Some(entityId), "event1"), Term(1)),
          ),
        )
        createLeaderData(currentTerm = Term(1), log = log)
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(1))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      setState(leader, Leader, leaderData)

      val oldState = getState(leader)
      oldState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))
      oldState.stateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(1))

      val succeeded = AppendEntriesSucceeded(Term(2), LogEntryIndex(2), followerMemberIndex)
      LoggingTestKit
        .warn(
          s"Unexpected message received: $succeeded (currentTerm: Term(1))",
        ).expect {
          leader ! succeeded
        }

      val newState = getState(leader)
      newState.stateName should be(Leader)
      newState.stateData.currentTerm should be(Term(1))
      newState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))
      newState.stateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(1))
    }

    "ignore AppendEntriesSucceeded with an older term" in {
      // If the leader handles AppendEntriesSucceeded, the next index and the match index for the follower(sender) will
      // change. This test verifies that AppendEntriesSucceeded with an older term won't trigger such changes.
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event1"), Term(2)),
          ),
        )
        createLeaderData(currentTerm = Term(2), log = log)
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(2))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      setState(leader, Leader, leaderData)

      val oldState = getState(leader)
      oldState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(3))
      oldState.stateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(2))

      leader ! AppendEntriesSucceeded(Term(1), LogEntryIndex(3), followerMemberIndex)

      val newState = getState(leader)
      newState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(3))
      newState.stateData.matchIndex.indexes(followerMemberIndex) should be(LogEntryIndex(2))
    }

    "handle AppendEntriesFailed with the current term and update next index for the follower(sender)" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event1"), Term(2)),
          ),
        )
        createLeaderData(currentTerm = Term(2), log = log)
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      setState(leader, Leader, leaderData)

      val followerMemberIndex = createUniqueMemberIndex()

      val oldState = getState(leader)
      oldState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(4))
      oldState.stateData.matchIndex.indexes should not contain key(followerMemberIndex)

      leader ! AppendEntriesFailed(Term(2), followerMemberIndex)

      val newState = getState(leader)
      newState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(3))
      newState.stateData.matchIndex.indexes should not contain key(followerMemberIndex)
    }

    "handle AppendEntriesFailed with the current term and not update next index for the follower(sender) if the next index is zero" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
            LogEntry(LogEntryIndex(3), EntityEvent(Some(entityId), "event1"), Term(2)),
          ),
        )
        createLeaderData(currentTerm = Term(2), log = log)
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      setState(leader, Leader, leaderData)

      val followerMemberIndex = createUniqueMemberIndex()

      getState(leader).stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(4))
      leader ! AppendEntriesFailed(Term(2), followerMemberIndex) // next index: 4 => 3
      leader ! AppendEntriesFailed(Term(2), followerMemberIndex) // next index: 3 => 2
      leader ! AppendEntriesFailed(Term(2), followerMemberIndex) // next index: 2 => 1
      leader ! AppendEntriesFailed(Term(2), followerMemberIndex) // next index: 1 => 0
      val oldState = getState(leader)
      oldState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(0))
      oldState.stateData.matchIndex.indexes should not contain key(followerMemberIndex)

      leader ! AppendEntriesFailed(Term(2), followerMemberIndex) // next index: 0 => 0

      val newState = getState(leader)
      newState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(0))
      newState.stateData.matchIndex.indexes should not contain key(followerMemberIndex)
    }

    "handle AppendEntriesFailed with a newer term, update its term, and become a follower" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          ),
        )
        createLeaderData(currentTerm = Term(1), log = log)
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      setState(leader, Leader, leaderData)

      val followerMemberIndex = createUniqueMemberIndex()
      leader ! AppendEntriesFailed(Term(2), followerMemberIndex)

      val state = getState(leader)
      state.stateName should be(Follower)
      state.stateData.currentTerm should be(Term(2))
      state.stateData.leaderMember should be(None)
    }

    "ignore AppendEntriesFailed with an older term" in {
      // If the leader handles AppendEntriesFailed, the next index for the follower(sender) will change. This test verifies
      // that AppendEntriesFailed with an older term won't trigger such changes.
      val leaderMemberIndex   = createUniqueMemberIndex()
      val followerMemberIndex = createUniqueMemberIndex()
      val leaderData = {
        val log = ReplicatedLog().truncateAndAppend(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(None, NoOp), Term(2)),
          ),
        )
        createLeaderData(currentTerm = Term(2), log = log)
          .syncLastLogIndex(followerMemberIndex, LogEntryIndex(1))
      }
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      setState(leader, Leader, leaderData)

      val oldState = getState(leader)
      oldState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))

      leader ! AppendEntriesFailed(Term(1), followerMemberIndex)

      val newState = getState(leader)
      newState.stateData.nextIndexFor(followerMemberIndex) should be(LogEntryIndex(2))
    }

    "become a follower and synchronize snapshots if it receives InstallSnapshot" in {
      val config   = RaftActorSpecBase.defaultRaftPersistenceConfig.withFallback(system.settings.config)
      val settings = ClusterReplicationSettingsImpl(config, Cluster(system).settings.Roles)

      val typeName       = TypeName.from("test")
      val shardId        = createUniqueShardId()
      val term1          = Term(1)
      val term2          = term1.next()
      val lastLogIndex   = LogEntryIndex(1)
      val srcMemberIndex = createUniqueMemberIndex()
      val dstMemberIndex = createUniqueMemberIndex()
      val region         = TestProbe()

      val eventStore = system.actorOf(EventStore.props(settings), "eventStore")
      def persistEvents(events: CompactionCompleted*): Unit = {
        eventStore ! EventStore.PersistEvents(events)
        expectMsg(Done)
      }

      val snapshotStore = planAutoKill {
        system.actorOf(
          ShardSnapshotStore.props(typeName, settings.raftSettings, srcMemberIndex),
          "srcSnapshotStore",
        )
      }
      def saveSnapshots(snapshots: Set[EntitySnapshot]): Unit = {
        snapshots.foreach { snapshot =>
          snapshotStore ! SnapshotProtocol.SaveSnapshot(snapshot, testActor)
        }
        receiveWhile(messages = snapshots.size) {
          case _: SnapshotProtocol.SaveSnapshotSuccess => Done
        }
      }

      val leader = createRaftActor(
        settings = settings.raftSettings,
        typeName = typeName,
        shardId = shardId,
        selfMemberIndex = dstMemberIndex,
        shardSnapshotStore = snapshotStore,
        region = region.ref,
      )
      setState(leader, Leader, createLeaderData(term1, log = ReplicatedLog()))

      persistEvents(
        CompactionCompleted(
          srcMemberIndex,
          shardId,
          snapshotLastLogTerm = term2,
          snapshotLastLogIndex = lastLogIndex,
          entityIds = Set(NormalizedEntityId("entity-1")),
        ),
      )

      val snapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(NormalizedEntityId("entity-1"), lastLogIndex), EntityState("dummy")),
      )
      saveSnapshots(snapshots)

      val installSnapshotCommand     = InstallSnapshot(shardId, term2, srcMemberIndex, term2, lastLogIndex)
      val expectedSuccessfulResponse = InstallSnapshotSucceeded(shardId, term2, lastLogIndex, dstMemberIndex)

      awaitAssert {
        leader ! installSnapshotCommand
        region.expectMsgType[ReplicationRegion.DeliverTo].message should be(expectedSuccessfulResponse)
        val state = getState(leader)
        state.stateName should be(Follower)
        state.stateData.currentTerm should be(term2)
        state.stateData.lastSnapshotStatus.snapshotLastTerm should be(term2)
        state.stateData.lastSnapshotStatus.snapshotLastLogIndex should be(lastLogIndex)
      }
      // InstallSnapshot is idempotent: InstallSnapshot will succeed again if it has already succeeded
      awaitAssert {
        leader ! installSnapshotCommand
        region.expectMsgType[ReplicationRegion.DeliverTo](max = 500.millis).message should be(
          expectedSuccessfulResponse,
        )
      }
    }

    "send AppendEntries to the follower when the leader has log entries that follower requires" in {
      val leaderIndex      = createUniqueMemberIndex()
      val follower1Index   = createUniqueMemberIndex()
      val follower2Index   = createUniqueMemberIndex()
      val region           = TestProbe()
      val replicationActor = TestProbe()
      val entityId         = NormalizedEntityId("test")
      val entityInstanceId = EntityInstanceId(1)
      val leader = createRaftActor(
        selfMemberIndex = leaderIndex,
        otherMemberIndexes = Set(follower1Index, follower2Index),
        region = region.ref,
        replicationActor = replicationActor.ref,
        entityId = entityId,
      )
      val term       = Term(1)
      val leaderData = createLeaderData(term)
      setState(leader, Candidate, leaderData)
      setState(leader, Leader, leaderData)

      region.fishForMessageN(messages = 2) {

        case msg @ ReplicationRegion.DeliverTo(`follower1Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(1)) =>
          cmd.leader should be(leaderIndex)
          cmd.term should be(term)
          cmd.prevLogIndex should be(LogEntryIndex.initial())
          cmd.prevLogTerm should be(Term.initial())
          inside(cmd.entries) {
            case Seq(logEntry) =>
              logEntry.index should be(LogEntryIndex(1))
              logEntry.event.event should be(NoOp)
              logEntry.event.entityId should be(None)
              logEntry.term should be(term)
          }
          leader ! AppendEntriesSucceeded(cmd.term, cmd.entries.last.index, msg.index)
          msg.index

        case msg @ ReplicationRegion.DeliverTo(`follower2Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(1)) =>
          cmd.leader should be(leaderIndex)
          cmd.term should be(term)
          cmd.prevLogIndex should be(LogEntryIndex.initial())
          cmd.prevLogTerm should be(Term.initial())
          inside(cmd.entries) {
            case Seq(logEntry) =>
              logEntry.index should be(LogEntryIndex(1))
              logEntry.event.event should be(NoOp)
              logEntry.event.entityId should be(None)
              logEntry.term should be(term)
          }
          // don't reply to the leader
          msg.index
      } should contain theSameElementsAs (Set(follower1Index, follower2Index))

      val event1 = "a"
      leader ! Replicate(event1, replicationActor.ref, entityId, entityInstanceId, system.deadLetters)

      region.fishForMessageN(messages = 2) {

        case msg @ ReplicationRegion.DeliverTo(`follower1Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(2)) =>
          cmd.leader should be(leaderIndex)
          cmd.term should be(term)
          cmd.prevLogIndex should be(LogEntryIndex(1))
          cmd.prevLogTerm should be(term)
          inside(cmd.entries) {
            case Seq(logEntry) =>
              logEntry.index should be(LogEntryIndex(2))
              logEntry.event.event should be(event1)
              logEntry.event.entityId should be(Some(entityId))
              logEntry.term should be(term)
          }
          leader ! AppendEntriesSucceeded(cmd.term, cmd.entries.last.index, msg.index)
          msg.index

        case msg @ ReplicationRegion.DeliverTo(`follower2Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(2)) =>
          cmd.leader should be(leaderIndex)
          cmd.term should be(term)
          cmd.prevLogIndex should be(LogEntryIndex.initial())
          cmd.prevLogTerm should be(Term.initial())
          inside(cmd.entries) {
            case Seq(logEntry1, logEntry2) =>
              logEntry1.index should be(LogEntryIndex(1))
              logEntry1.event.event should be(NoOp)
              logEntry1.event.entityId should be(None)
              logEntry1.term should be(term)

              logEntry2.index should be(LogEntryIndex(2))
              logEntry2.event.event should be(event1)
              logEntry2.event.entityId should be(Some(entityId))
              logEntry2.term should be(term)
          }
          // don't reply to the leader
          msg.index
      } should contain theSameElementsAs (Set(follower1Index, follower2Index))
    }

    "send at most `max-append-entries-batch-size` AppendEntries messages to followers on HeartbeatTimeout if it has more logs than `max-append-entries-size`" in {
      // The leader will send 3 AppendEntries messages to follower1.
      // The leader will send 2 AppendEntries messages to follower2.
      val leaderIndex    = createUniqueMemberIndex()
      val follower1Index = createUniqueMemberIndex()
      val follower2Index = createUniqueMemberIndex()
      val regionProbe    = TestProbe()
      val shardId        = createUniqueShardId()
      val leader = {
        val testConfig = ConfigFactory.parseString(
          """
            |lerna.akka.entityreplication.raft {
            |  # Heartbeat never happen for simplicity of this test case.
            |  heartbeat-interval = 1000s
            |  max-append-entries-size = 2
            |  max-append-entries-batch-size = 3
            |}
            |""".stripMargin,
        )
        val customSettings = RaftSettings(testConfig.withFallback(defaultRaftConfig))
        customSettings.heartbeatInterval shouldBe 1000.seconds
        customSettings.maxAppendEntriesSize shouldBe 2
        customSettings.maxAppendEntriesBatchSize shouldBe 3
        createRaftActor(
          shardId = shardId,
          selfMemberIndex = leaderIndex,
          otherMemberIndexes = Set(follower1Index, follower2Index),
          region = regionProbe.ref,
          settings = customSettings,
        )
      }
      val currentTerm = Term(2)
      val logEntries = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "a"), Term(1)),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "b"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "c"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "d"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "e"), Term(2)),
        LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "f"), Term(2)),
        LogEntry(LogEntryIndex(7), EntityEvent(Option(entityId), "g"), Term(2)),
        LogEntry(LogEntryIndex(8), EntityEvent(None, NoOp), Term(2)),
      )
      val logEntryByIndex = logEntries.map(entry => entry.index -> entry).toMap
      val leaderData = {
        val replicatedLog = ReplicatedLog().truncateAndAppend(logEntries)
        createLeaderData(currentTerm, replicatedLog, commitIndex = LogEntryIndex(1))
          .syncLastLogIndex(follower1Index, LogEntryIndex(1))
          .syncLastLogIndex(follower2Index, LogEntryIndex(5))
      }
      leaderData.nextIndexFor(follower1Index) shouldBe LogEntryIndex(2)
      leaderData.nextIndexFor(follower2Index) shouldBe LogEntryIndex(6)
      setState(leader, Leader, leaderData)

      def assertEquals(value: AppendEntries, expected: AppendEntries, clue: String): Unit = {
        withClue(clue) {
          value shouldBe expected
          // The following verification is needed since `LogEntry.equals` does not take `event` field into account.
          value.entries.zip(expected.entries).foreach {
            case (entry, expectedEntry) =>
              entry.event shouldBe expectedEntry.event
          }
        }
      }

      // Sending HeartbeatTimeout triggers that leader send AppendEntries messages to followers.
      leader ! HeartbeatTimeout

      // Fish AppendEntries messages sent by the leader.
      val appendEntriesSentByLeader = regionProbe.fishForMessageN(messages = 3 + 2) {
        case ReplicationRegion.DeliverTo(followerIndex, cmd: AppendEntries) =>
          val appendEntries = followerIndex match {
            case `follower1Index` => cmd
            case `follower2Index` => cmd
            case otherIndex       => fail(s"Got an AppendEntries with unexpected memberIndex($otherIndex)")
          }
          followerIndex -> appendEntries
      }
      val appendEntriesFollower1Received = appendEntriesSentByLeader.filter(_._1 == follower1Index).map(_._2)
      val appendEntriesFollower2Received = appendEntriesSentByLeader.filter(_._1 == follower2Index).map(_._2)

      // Verify all AppendEntries messages.
      appendEntriesFollower1Received.size shouldBe 3
      assertEquals(
        appendEntriesFollower1Received(0),
        AppendEntries(
          shardId,
          currentTerm,
          leaderIndex,
          prevLogIndex = LogEntryIndex(1),
          prevLogTerm = Term(1),
          entries = Seq(
            logEntryByIndex(LogEntryIndex(2)),
            logEntryByIndex(LogEntryIndex(3)),
          ),
          leaderCommit = LogEntryIndex(1),
        ),
        "The 1st AppendEntries follower1 received: ",
      )
      assertEquals(
        appendEntriesFollower1Received(1),
        AppendEntries(
          shardId,
          currentTerm,
          leaderIndex,
          prevLogIndex = LogEntryIndex(3),
          prevLogTerm = Term(1),
          entries = Seq(
            logEntryByIndex(LogEntryIndex(4)),
            logEntryByIndex(LogEntryIndex(5)),
          ),
          leaderCommit = LogEntryIndex(1),
        ),
        "The 2nd AppendEntries follower1 received: ",
      )
      assertEquals(
        appendEntriesFollower1Received(2),
        AppendEntries(
          shardId,
          currentTerm,
          leaderIndex,
          prevLogIndex = LogEntryIndex(5),
          prevLogTerm = Term(2),
          entries = Seq(
            logEntryByIndex(LogEntryIndex(6)),
            logEntryByIndex(LogEntryIndex(7)),
          ),
          leaderCommit = LogEntryIndex(1),
        ),
        "The 3rd AppendEntries follower1 received: ",
      )

      appendEntriesFollower2Received.size shouldBe 2
      assertEquals(
        appendEntriesFollower2Received(0),
        AppendEntries(
          shardId,
          currentTerm,
          leaderIndex,
          prevLogIndex = LogEntryIndex(5),
          prevLogTerm = Term(2),
          entries = Seq(
            logEntryByIndex(LogEntryIndex(6)),
            logEntryByIndex(LogEntryIndex(7)),
          ),
          leaderCommit = LogEntryIndex(1),
        ),
        "The 1st AppendEntries follower2 received: ",
      )
      assertEquals(
        appendEntriesFollower2Received(1),
        AppendEntries(
          shardId,
          currentTerm,
          leaderIndex,
          prevLogIndex = LogEntryIndex(7),
          prevLogTerm = Term(2),
          entries = Seq(
            logEntryByIndex(LogEntryIndex(8)),
          ),
          leaderCommit = LogEntryIndex(1),
        ),
        "The 2nd AppendEntries follower2 received: ",
      )

      // The leader should send no more AppendEntries on this HeartbeatTimeout.
      assertThrows[AssertionError] {
        regionProbe.fishForMessage(hint = "The leader sent an unexpected AppendEntries") {
          case ReplicationRegion.DeliverTo(_, _: AppendEntries) => true
          case _                                                => false
        }
      }
    }

    "send InstallSnapshot to the follower when the leader loses logs that the follower requires by compaction" in {
      val leaderIndex      = createUniqueMemberIndex()
      val follower1Index   = createUniqueMemberIndex()
      val follower2Index   = createUniqueMemberIndex()
      val region           = TestProbe()
      val snapshotStore    = TestProbe()
      val replicationActor = TestProbe()
      val commitLogStore   = TestProbe()
      val entityId         = NormalizedEntityId("test")
      val entityInstanceId = EntityInstanceId(1)
      val config = ConfigFactory.parseString {
        """
        lerna.akka.entityreplication.raft.compaction {
          log-size-check-interval = 10ms
          log-size-threshold = 2
          preserve-log-size = 1
        }
        """
      }
      val leader = createRaftActor(
        shardId = shardId,
        selfMemberIndex = leaderIndex,
        otherMemberIndexes = Set(follower1Index, follower2Index),
        region = region.ref,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        entityId = entityId,
        settings = RaftSettings(config.withFallback(defaultRaftConfig)),
        commitLogStore = commitLogStore.ref,
      )
      val term       = Term(1)
      val leaderData = createLeaderData(term)
      setState(leader, Candidate, leaderData)
      setState(leader, Leader, leaderData)

      // To let the leader know the first eventSourcingIndex, CommitLogStore should handle AppendCommittedEntries.
      commitLogStore.expectMsg(CommitLogStoreActor.AppendCommittedEntries(shardId, entries = Seq.empty))
      commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(LogEntryIndex(0)))

      region.fishForMessageN(messages = 2) {

        case msg @ ReplicationRegion.DeliverTo(`follower1Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(1)) =>
          // LogEntryIndex(1) (NoOp) will be committed
          inside(cmd.entries) {
            case Seq(logEntry) =>
              logEntry.event.event should be(NoOp)
          }
          leader ! AppendEntriesSucceeded(cmd.term, cmd.entries.last.index, msg.index)
          msg.index

        case msg @ ReplicationRegion.DeliverTo(`follower2Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(1)) =>
          // don't reply to the leader
          msg.index
      } should contain theSameElementsAs (Set(follower1Index, follower2Index))

      val event1 = "a"
      leader ! Replicate(event1, replicationActor.ref, entityId, entityInstanceId, system.deadLetters)

      region.fishForMessageN(messages = 2) {

        case msg @ ReplicationRegion.DeliverTo(`follower1Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(2)) =>
          // LogEntryIndex(2) will be committed
          inside(cmd.entries) {
            case Seq(logEntry) =>
              logEntry.event.event should be(event1)
          }
          leader ! AppendEntriesSucceeded(cmd.term, cmd.entries.last.index, msg.index)
          msg.index

        case msg @ ReplicationRegion.DeliverTo(`follower2Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(2)) =>
          // don't reply to the leader
          msg.index
      } should contain theSameElementsAs (Set(follower1Index, follower2Index))

      // To advance compaction target entries, CommitLogStore should handle AppendCommittedEntries.
      commitLogStore.fishForSpecificMessage() {
        case CommitLogStoreActor.AppendCommittedEntries(`shardId`, entries)
            if entries.lastOption.exists(_.index == LogEntryIndex(2)) =>
          commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(LogEntryIndex(2)))
      }

      // compaction started
      replicationActor.fishForSpecificMessage() {
        case _: RaftProtocol.TakeSnapshot =>
          leader ! RaftProtocol.Snapshot(EntitySnapshotMetadata(entityId, LogEntryIndex(2)), EntityState("state"))
      }
      snapshotStore.fishForSpecificMessage() {
        case cmd: SnapshotProtocol.SaveSnapshot =>
          leader ! SnapshotProtocol.SaveSnapshotSuccess(cmd.snapshot.metadata)
      }

      val event2 = "b"
      leader ! Replicate(event2, replicationActor.ref, entityId, entityInstanceId, system.deadLetters)

      region.fishForMessageN(messages = 2) {

        case msg @ ReplicationRegion.DeliverTo(`follower1Index`, cmd: AppendEntries)
            if cmd.entries.lastOption.exists(_.index == LogEntryIndex(3)) =>
          // LogEntryIndex(3) will be committed
          inside(cmd.entries) {
            case Seq(logEntry) =>
              logEntry.event.event should be(event2)
          }
          leader ! AppendEntriesSucceeded(cmd.term, cmd.entries.last.index, msg.index)
          msg.index

        case msg @ ReplicationRegion.DeliverTo(`follower2Index`, cmd: InstallSnapshot) =>
          cmd.term should be(term)
          cmd.srcLatestSnapshotLastLogLogIndex should be(LogEntryIndex(2))
          cmd.srcLatestSnapshotLastLogTerm should be(term)
          leader ! InstallSnapshotSucceeded(shardId, term, cmd.srcLatestSnapshotLastLogLogIndex, follower2Index)
          msg.index
      } should contain theSameElementsAs (Set(follower1Index, follower2Index))
    }

    "reply ReplicationFailed to replicationActor and log warn message if replication is in progress" in {
      val replicationActor1 = TestProbe()
      val replicationActor2 = TestProbe()
      val entityId1         = NormalizedEntityId("test-1")
      val entityId2         = NormalizedEntityId("test-2")
      val entityInstanceId  = EntityInstanceId(1)

      val leader     = createRaftActor()
      val term       = Term(1)
      val leaderData = createLeaderData(term)
      setState(leader, Candidate, leaderData)
      setState(leader, Leader, leaderData)

      leader ! Replicate(
        event = "a",
        replyTo = replicationActor1.ref,
        entityId1,
        entityInstanceId,
        originSender = system.deadLetters,
      )
      replicationActor1.expectNoMessage()

      leader ! Replicate(
        event = "b",
        replicationActor2.ref,
        entityId2,
        entityInstanceId,
        originSender = system.deadLetters,
      )
      replicationActor2.expectNoMessage()

      LoggingTestKit
        .warn(
          "Failed to replicate the event (java.lang.String) since an uncommitted event exists for the entity (entityId: test-1). Replicating new events is allowed after the event is committed",
        ).expect {
          leader ! Replicate(
            event = "c", // java.lang.String
            replicationActor1.ref,
            entityId1,
            entityInstanceId,
            originSender = system.deadLetters,
          )
          replicationActor1.expectMsg(ReplicationFailed)
        }
    }
  }

  private[this] def createLeaderData(
      currentTerm: Term,
      log: ReplicatedLog = ReplicatedLog(),
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      lastApplied: LogEntryIndex = LogEntryIndex.initial(),
      nextIndex: NextIndex = NextIndex(ReplicatedLog()),
      matchIndex: MatchIndex = MatchIndex(),
  ): RaftMemberData =
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = log,
      commitIndex = commitIndex,
      lastApplied = lastApplied,
      nextIndex = Some(nextIndex),
      matchIndex = matchIndex,
    ).initializeLeaderData()
}
