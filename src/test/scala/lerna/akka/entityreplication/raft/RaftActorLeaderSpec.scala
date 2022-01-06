package lerna.akka.entityreplication.raft

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.{ ClusterReplicationSettings, ReplicationRegion }
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftProtocol.Replicate
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

class RaftActorLeaderSpec extends TestKit(ActorSystem()) with RaftActorSpecBase with Inside {

  import RaftActor._

  private[this] val entityId = NormalizedEntityId.from("test-entity")
  private[this] val shardId  = NormalizedShardId.from("test-shard")

  "Leader" should {

    "他のメンバーの古い Term の RequestVote には Deny する" in {
      val leader = createRaftActor()
      val term1  = Term.initial()
      val term2  = term1.next()
      setState(leader, Leader, createLeaderData(term2))

      val anotherMemberIndex = createUniqueMemberIndex()

      leader ! RequestVote(shardId, term1, anotherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteDenied(term2))
    }

    "他のメンバーの進んだ Term の RequestVote には Accept して Follower になる" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val term1 = Term.initial()
      setState(leader, Leader, createLeaderData(term1))

      val anotherMemberIndex = createUniqueMemberIndex()
      val term2              = term1.next()
      leader ! RequestVote(shardId, term2, anotherMemberIndex, LogEntryIndex.initial(), Term.initial())
      expectMsg(RequestVoteAccepted(term2, leaderMemberIndex))
      getState(leader).stateName should be(Follower)
    }

    "deny RequestVote if lastLogIndex is older than own even if the request has same lastLogTerm" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val candidateMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! RequestVote(shardId, term2, candidateMemberIndex, lastLogIndex = index1, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term2))
    }

    "deny RequestVote if lastLogTerm is older than own even if the request has newer lastLogIndex than own" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val candidateMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val term3                = Term(3)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val index3               = LogEntryIndex(3)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term2),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term2, log))

      leader ! RequestVote(shardId, term3, candidateMemberIndex, lastLogIndex = index3, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term3))
    }

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

      getState(leader).stateName should be(Follower)
    }

    "コマンドを ReplicationActor に転送する" ignore {}

    "AppendEntries が新しい Term を持っていて、prevLogIndex/prevLogTerm に一致するログエントリがある場合は AppendEntriesSucceeded" in {
      val leaderMemberIndex = createUniqueMemberIndex()
      val leader = createRaftActor(
        selfMemberIndex = leaderMemberIndex,
      )
      val newLeaderMemberIndex = createUniqueMemberIndex()
      val term1                = Term(1)
      val term2                = Term(2)
      val index1               = LogEntryIndex(1)
      val index2               = LogEntryIndex(2)
      val logEntries = Seq(
        LogEntry(index1, EntityEvent(Option(entityId), "a"), term1),
        LogEntry(index2, EntityEvent(Option(entityId), "b"), term1),
      )
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index2, term1, logEntries)
      expectMsg(AppendEntriesSucceeded(term2, index2, leaderMemberIndex))
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
      val log = ReplicatedLog().merge(logEntries, LogEntryIndex.initial())
      setState(leader, Leader, createLeaderData(term1, log))

      leader ! createAppendEntries(shardId, term2, newLeaderMemberIndex, index3, term1, logEntries)
      expectMsg(AppendEntriesFailed(term2, leaderMemberIndex))
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
      val log = ReplicatedLog().merge(followerLogEntries, LogEntryIndex.initial())
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
      val log        = ReplicatedLog().merge(logEntries1, LogEntryIndex.initial())
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
      val log        = ReplicatedLog().merge(term1LogEntries, LogEntryIndex.initial())
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

    "RequestVote の Term が新しくてもログが古い場合は否認する" in {
      val leader    = createRaftActor()
      val term1     = Term.initial()
      val leaderLog = ReplicatedLog().append(EntityEvent(Option(entityId), "dummy"), term1)
      setState(leader, Leader, createLeaderData(term1, log = leaderLog))

      val anotherMemberIndex = createUniqueMemberIndex()
      val term2              = term1.next()
      val lastLogIndex       = LogEntryIndex.initial()
      leader ! RequestVote(shardId, term2, anotherMemberIndex, lastLogIndex, lastLogTerm = term1)
      expectMsg(RequestVoteDenied(term2))
    }

    "become a follower and synchronize snapshots if it receives InstallSnapshot" in {
      val typeName       = TypeName.from("test")
      val shardId        = createUniqueShardId()
      val term1          = Term(1)
      val term2          = term1.next()
      val lastLogIndex   = LogEntryIndex(1)
      val srcMemberIndex = createUniqueMemberIndex()
      val dstMemberIndex = createUniqueMemberIndex()
      val region         = TestProbe()
      val snapshotStore = planAutoKill {
        system.actorOf(
          ShardSnapshotStore.props(typeName, settings.raftSettings, srcMemberIndex),
          "srcSnapshotStore",
        )
      }
      val leader = createRaftActor(
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
      saveSnapshots(snapshots, snapshotStore)

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
      leader ! installSnapshotCommand
      region.expectMsgType[ReplicationRegion.DeliverTo].message should be(expectedSuccessfulResponse)
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

    "send InstallSnapshot to the follower when the leader loses logs that the follower requires by compaction" in {
      val leaderIndex      = createUniqueMemberIndex()
      val follower1Index   = createUniqueMemberIndex()
      val follower2Index   = createUniqueMemberIndex()
      val region           = TestProbe()
      val snapshotStore    = TestProbe()
      val replicationActor = TestProbe()
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
        selfMemberIndex = leaderIndex,
        otherMemberIndexes = Set(follower1Index, follower2Index),
        region = region.ref,
        shardSnapshotStore = snapshotStore.ref,
        replicationActor = replicationActor.ref,
        entityId = entityId,
        settings = RaftSettings(config.withFallback(defaultRaftConfig)),
      )
      val term       = Term(1)
      val leaderData = createLeaderData(term)
      setState(leader, Candidate, leaderData)
      setState(leader, Leader, leaderData)

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
  }

  private[this] val settings = ClusterReplicationSettings.create(system)

  private[this] val eventStore = system.actorOf(EventStore.props(settings), "eventStore")

  private[this] def persistEvents(events: CompactionCompleted*): Unit = {
    eventStore ! EventStore.PersistEvents(events)
    expectMsg(Done)
  }

  private[this] def saveSnapshots(snapshots: Set[EntitySnapshot], snapshotStore: ActorRef): Unit = {
    snapshots.foreach { snapshot =>
      snapshotStore ! SnapshotProtocol.SaveSnapshot(snapshot, testActor)
    }
    receiveWhile(messages = snapshots.size) {
      case _: SnapshotProtocol.SaveSnapshotSuccess => Done
    }
  }

  private[this] def createLeaderData(
      currentTerm: Term,
      log: ReplicatedLog = ReplicatedLog(),
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      nextIndex: NextIndex = NextIndex(ReplicatedLog()),
      matchIndex: MatchIndex = MatchIndex(),
  ): RaftMemberData =
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = log,
      commitIndex = commitIndex,
      nextIndex = Some(nextIndex),
      matchIndex = matchIndex,
    ).initializeLeaderData()
}
