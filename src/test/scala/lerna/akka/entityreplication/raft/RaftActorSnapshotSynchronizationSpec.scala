package lerna.akka.entityreplication.raft

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ typed, ActorSystem, Status }
import akka.persistence.inmemory.extension.{ InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.{ ClusterReplicationSettings, ReplicationRegion }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.RaftActor.{ CompactionCompleted, ElectionTimeout, Follower }
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import lerna.akka.entityreplication.util.{ RaftEventJournalTestKit, RaftSnapshotStoreTestKit }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Inside._

class RaftActorSnapshotSynchronizationSpec
    extends TestKit(ActorSystem())
    with RaftActorSpecBase
    with BeforeAndAfterEach
    with ImplicitSender {

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped

  private val settings                       = ClusterReplicationSettings.create(system)
  private val typeName                       = TypeName.from("test-type-1")
  private val shardId                        = createUniqueShardId()
  private val leaderMemberIndex              = createUniqueMemberIndex()
  private val leaderRaftSnapshotStoreTestKit = RaftSnapshotStoreTestKit(system, typeName, leaderMemberIndex, settings)
  private val raftEventJournalTestKit        = RaftEventJournalTestKit(system, settings)

  override def beforeEach(): Unit = {
    super.beforeEach()
    // clear storage
    val storage = StorageExtension(system)
    storage.journalStorage ! InMemoryJournalStorage.ClearJournal
    storage.snapshotStorage ! InMemorySnapshotStorage.ClearSnapshots
    receiveWhile(messages = 2) {
      case _: Status.Success => Done
    } should have length 2
    // reset SnapshotStore
    leaderRaftSnapshotStoreTestKit.reset()
  }

  "RaftActor snapshot synchronization" should {

    val raftConfig = ConfigFactory
      .parseString("""
                     | lerna.akka.entityreplication.raft {
                     |   election-timeout = 99999s
                     | }
                     |""".stripMargin).withFallback(ConfigFactory.load())

    "make the follower to reject AppendEntries until completing synchronization" in {
      /* prepare */
      val snapshotStore       = TestProbe()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        typeName = typeName,
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        settings = RaftSettings(raftConfig),
      )
      val leaderSnapshotTerm       = Term(1)
      val leaderSnapshotLogIndex   = LogEntryIndex(3)
      var currentLeaderTerm        = Term(2)
      var currentLeaderMemberIndex = createUniqueMemberIndex()
      val entityId                 = NormalizedEntityId("test-entity")
      val leaderSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entityId, leaderSnapshotLogIndex), EntityState("state-1")),
      )
      leaderRaftSnapshotStoreTestKit.saveSnapshots(leaderSnapshots)
      raftEventJournalTestKit.persistEvents(
        CompactionCompleted(leaderMemberIndex, shardId, leaderSnapshotTerm, leaderSnapshotLogIndex, Set(entityId)),
      )
      /*
       * Following methods simulate that new leader has been elected by any reasons.
       */
      def newLeaderTerm(): Term               = currentLeaderTerm.next()
      def newLeaderMemberIndex(): MemberIndex = createUniqueMemberIndex()
      /* check */
      follower ! InstallSnapshot(
        shardId,
        term = currentLeaderTerm,
        srcMemberIndex = leaderMemberIndex,
        srcLatestSnapshotLastLogTerm = leaderSnapshotTerm,
        srcLatestSnapshotLastLogLogIndex = leaderSnapshotLogIndex,
      )
      inside(getState(follower)) { state =>
        state.stateData.lastSnapshotStatus.targetSnapshotLastTerm should be(leaderSnapshotTerm)
        state.stateData.lastSnapshotStatus.targetSnapshotLastLogIndex should be(leaderSnapshotLogIndex)
        state.stateData.lastSnapshotStatus.isDirty should be(true)
        // should not update ReplicatedLog to prevent the RaftActor from becoming a leader during snapshot synchronization
        state.stateData.replicatedLog.lastLogTerm should be(Term.initial())
        state.stateData.replicatedLog.lastLogIndex should be(LogEntryIndex.initial())
      }
      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = newLeaderMemberIndex()
      // This AppendEntries will be denied since snapshot synchronization is in progress
      // and the (prevLogIndex, prevLogTerm) do not match (targetSnapshotLastTerm, targetSnapshotLastLogIndex)
      follower ! AppendEntries(
        shardId,
        currentLeaderTerm,
        currentLeaderMemberIndex,
        prevLogIndex = leaderSnapshotLogIndex.plus(1),
        prevLogTerm = leaderSnapshotTerm,
        entries = Seq(
          LogEntry(leaderSnapshotLogIndex.plus(2), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(3), EntityEvent(Option(entityId), "event-1"), leaderSnapshotTerm),
        ),
        leaderCommit = leaderSnapshotLogIndex.plus(10),
      )
      inside(expectMsgType[AppendEntriesFailed]) {
        case AppendEntriesFailed(term, sender) =>
          term should be(currentLeaderTerm)
          sender should be(followerMemberIndex)
      }
      inside(getState(follower)) { state =>
        state.stateName should be(Follower)
        state.stateData.currentTerm should be(currentLeaderTerm)
        state.stateData.leaderMember should be(Option(currentLeaderMemberIndex))
      }

      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = newLeaderMemberIndex()
      // This AppendEntries will be ignored since snapshot synchronization is in progress
      // and the (prevLogIndex, prevLogTerm) match (targetSnapshotLastTerm, targetSnapshotLastLogIndex)
      follower ! AppendEntries(
        shardId,
        currentLeaderTerm,
        currentLeaderMemberIndex,
        prevLogIndex = leaderSnapshotLogIndex,
        prevLogTerm = leaderSnapshotTerm,
        entries = Seq(
          LogEntry(leaderSnapshotLogIndex.plus(1), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(2), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(3), EntityEvent(Option(entityId), "event-1"), leaderSnapshotTerm),
        ),
        leaderCommit = leaderSnapshotLogIndex.plus(10),
      )
      expectNoMessage()
      inside(getState(follower)) { state =>
        state.stateName should be(Follower)
        state.stateData.currentTerm should be(currentLeaderTerm)
        state.stateData.leaderMember should be(Option(currentLeaderMemberIndex))
      }

      LoggingTestKit.info("Snapshot synchronization completed").expect {
        snapshotStore.receiveWhile(messages = 1) {
          case msg: SaveSnapshot =>
            msg.replyTo ! SaveSnapshotSuccess(msg.snapshot.metadata)
        } should have length 1
      }
      inside(getState(follower)) { state =>
        state.stateData.lastSnapshotStatus.snapshotLastTerm should be(leaderSnapshotTerm)
        state.stateData.lastSnapshotStatus.snapshotLastLogIndex should be(leaderSnapshotLogIndex)
        state.stateData.lastSnapshotStatus.targetSnapshotLastTerm should be(leaderSnapshotTerm)
        state.stateData.lastSnapshotStatus.targetSnapshotLastLogIndex should be(leaderSnapshotLogIndex)
        state.stateData.lastSnapshotStatus.isDirty should be(false)
      }
      // This AppendEntries will be accepted since the snapshot synchronization completed
      // and the (prevLogIndex, prevLogTerm) match (ancestorLastTerm, ancestorLastIndex)
      follower ! AppendEntries(
        shardId,
        currentLeaderTerm,
        currentLeaderMemberIndex,
        prevLogIndex = leaderSnapshotLogIndex,
        prevLogTerm = leaderSnapshotTerm,
        entries = Seq(
          LogEntry(leaderSnapshotLogIndex.plus(1), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(2), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(3), EntityEvent(Option(entityId), "event-1"), leaderSnapshotTerm),
        ),
        leaderCommit = leaderSnapshotLogIndex.plus(10),
      )
      expectMsgType[AppendEntriesSucceeded]
    }

    "make the candidate to reject AppendEntries until completing synchronization" in {
      /* prepare */
      val snapshotStore       = TestProbe()
      val followerMemberIndex = createUniqueMemberIndex()
      val followerThatWillBeCandidate = createRaftActor(
        typeName = typeName,
        shardId = shardId,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        settings = RaftSettings(raftConfig),
      )
      val leaderSnapshotTerm       = Term(1)
      val leaderSnapshotLogIndex   = LogEntryIndex(3)
      var currentLeaderTerm        = Term(2)
      var currentLeaderMemberIndex = createUniqueMemberIndex()
      val entityId                 = NormalizedEntityId("test-entity")
      val leaderSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entityId, leaderSnapshotLogIndex), EntityState("state-1")),
      )
      leaderRaftSnapshotStoreTestKit.saveSnapshots(leaderSnapshots)
      raftEventJournalTestKit.persistEvents(
        CompactionCompleted(leaderMemberIndex, shardId, leaderSnapshotTerm, leaderSnapshotLogIndex, Set(entityId)),
      )
      /*
       * Following methods simulate that new leader has been elected by any reasons.
       * NOTE:
       *   Becoming candidate increments term value of the candidate.
       *   The leader updates own term by receiving AppendEntriesFailed from the candidate.
       *   `newLeaderTerm()` simulates the updation and allows to omit the process that the leader receive AppendEntriesFailed.
       */
      def newLeaderTerm(): Term               = getState(followerThatWillBeCandidate).stateData.currentTerm.next()
      def newLeaderMemberIndex(): MemberIndex = createUniqueMemberIndex()
      /* check */
      followerThatWillBeCandidate ! InstallSnapshot(
        shardId,
        term = currentLeaderTerm,
        srcMemberIndex = leaderMemberIndex,
        srcLatestSnapshotLastLogTerm = leaderSnapshotTerm,
        srcLatestSnapshotLastLogLogIndex = leaderSnapshotLogIndex,
      )
      inside(getState(followerThatWillBeCandidate)) { state =>
        state.stateData.lastSnapshotStatus.isDirty should be(true)
      }
      followerThatWillBeCandidate ! ElectionTimeout
      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = newLeaderMemberIndex()
      // This AppendEntries will be denied since snapshot synchronization is in progress
      // and the (prevLogIndex, prevLogTerm) do not match (targetSnapshotLastTerm, targetSnapshotLastLogIndex)
      followerThatWillBeCandidate ! AppendEntries(
        shardId,
        currentLeaderTerm,
        currentLeaderMemberIndex,
        prevLogIndex = leaderSnapshotLogIndex.plus(1),
        prevLogTerm = leaderSnapshotTerm,
        entries = Seq(
          LogEntry(leaderSnapshotLogIndex.plus(2), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(3), EntityEvent(Option(entityId), "event-1"), leaderSnapshotTerm),
        ),
        leaderCommit = leaderSnapshotLogIndex.plus(10),
      )
      inside(expectMsgType[AppendEntriesFailed]) {
        case AppendEntriesFailed(term, sender) =>
          term should be(currentLeaderTerm)
          sender should be(followerMemberIndex)
      }
      inside(getState(followerThatWillBeCandidate)) { state =>
        state.stateName should be(Follower)
        state.stateData.currentTerm should be(currentLeaderTerm)
        state.stateData.leaderMember should be(Option(currentLeaderMemberIndex))
      }

      followerThatWillBeCandidate ! ElectionTimeout
      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = newLeaderMemberIndex()
      // This AppendEntries will be ignored since snapshot synchronization is in progress
      // and the (prevLogIndex, prevLogTerm) match (targetSnapshotLastTerm, targetSnapshotLastLogIndex)
      followerThatWillBeCandidate ! AppendEntries(
        shardId,
        currentLeaderTerm,
        currentLeaderMemberIndex,
        prevLogIndex = leaderSnapshotLogIndex,
        prevLogTerm = leaderSnapshotTerm,
        entries = Seq(
          LogEntry(leaderSnapshotLogIndex.plus(1), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(2), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(3), EntityEvent(Option(entityId), "event-1"), leaderSnapshotTerm),
        ),
        leaderCommit = leaderSnapshotLogIndex.plus(10),
      )
      expectNoMessage()
      inside(getState(followerThatWillBeCandidate)) { state =>
        state.stateName should be(Follower)
        state.stateData.currentTerm should be(currentLeaderTerm)
        state.stateData.leaderMember should be(Option(currentLeaderMemberIndex))
      }

      LoggingTestKit.info("Snapshot synchronization completed").expect {
        snapshotStore.receiveWhile(messages = 1) {
          case msg: SaveSnapshot =>
            msg.replyTo ! SaveSnapshotSuccess(msg.snapshot.metadata)
        } should have length 1
      }
      followerThatWillBeCandidate ! ElectionTimeout
      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = newLeaderMemberIndex()
      // This AppendEntries will be accepted since the snapshot synchronization completed
      // and the (prevLogIndex, prevLogTerm) match (ancestorLastTerm, ancestorLastIndex)
      followerThatWillBeCandidate ! AppendEntries(
        shardId,
        currentLeaderTerm,
        currentLeaderMemberIndex,
        prevLogIndex = leaderSnapshotLogIndex,
        prevLogTerm = leaderSnapshotTerm,
        entries = Seq(
          LogEntry(leaderSnapshotLogIndex.plus(1), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(2), EntityEvent(None, NoOp), leaderSnapshotTerm),
          LogEntry(leaderSnapshotLogIndex.plus(3), EntityEvent(Option(entityId), "event-1"), leaderSnapshotTerm),
        ),
        leaderCommit = leaderSnapshotLogIndex.plus(10),
      )
      expectMsgType[AppendEntriesSucceeded]
    }

    "prevent to process InstallSnapshot commands that indicates old snapshots" in {
      /* prepare */
      val snapshotStore       = TestProbe()
      val region              = TestProbe()
      val followerMemberIndex = createUniqueMemberIndex()
      val follower = createRaftActor(
        typeName = typeName,
        shardId = shardId,
        region = region.ref,
        selfMemberIndex = followerMemberIndex,
        shardSnapshotStore = snapshotStore.ref,
        settings = RaftSettings(raftConfig),
      )
      val leaderSnapshotTerm1      = Term(1)
      val leaderSnapshotTerm2      = Term(3)
      val leaderSnapshotLogIndex1  = LogEntryIndex(10)
      val leaderSnapshotLogIndex2  = LogEntryIndex(20)
      var currentLeaderTerm        = Term(4)
      var currentLeaderMemberIndex = leaderMemberIndex
      val entityId                 = NormalizedEntityId("test-entity")
      val leaderSnapshots = Set(
        EntitySnapshot(EntitySnapshotMetadata(entityId, leaderSnapshotLogIndex2), EntityState("state-1")),
      )
      leaderRaftSnapshotStoreTestKit.saveSnapshots(leaderSnapshots)
      raftEventJournalTestKit.persistEvents(
        CompactionCompleted(leaderMemberIndex, shardId, leaderSnapshotTerm1, leaderSnapshotLogIndex1, Set(entityId)),
        CompactionCompleted(leaderMemberIndex, shardId, leaderSnapshotTerm2, leaderSnapshotLogIndex2, Set(entityId)),
      )
      /*
       * Following methods simulate that new leader has been elected by any reasons.
       */
      def newLeaderTerm(): Term               = currentLeaderTerm.next()
      def newLeaderMemberIndex(): MemberIndex = createUniqueMemberIndex()
      /* check */
      LoggingTestKit.warn("Snapshot synchronization aborted").expect {
        // This InstallSnapshot fails since second CompactionCompleted will be found
        follower ! InstallSnapshot(
          shardId,
          term = currentLeaderTerm,
          srcMemberIndex = currentLeaderMemberIndex,
          srcLatestSnapshotLastLogTerm = leaderSnapshotTerm1,
          srcLatestSnapshotLastLogLogIndex = leaderSnapshotLogIndex1,
        )
      }
      inside(getState(follower)) { state =>
        state.stateData.lastSnapshotStatus.targetSnapshotLastTerm should be(leaderSnapshotTerm1)
        state.stateData.lastSnapshotStatus.targetSnapshotLastLogIndex should be(leaderSnapshotLogIndex1)
      }
      expectNoMessage() // because the process failed

      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = newLeaderMemberIndex()
      // This InstallSnapshot will be ignored since it indicates older snapshots than the previous command
      follower ! InstallSnapshot(
        shardId,
        term = currentLeaderTerm,
        srcMemberIndex = currentLeaderMemberIndex,
        srcLatestSnapshotLastLogTerm = leaderSnapshotTerm1,
        srcLatestSnapshotLastLogLogIndex = leaderSnapshotLogIndex1.prev(),
      )
      expectNoMessage()
      inside(getState(follower)) { state =>
        state.stateName should be(Follower)
        state.stateData.currentTerm should be(currentLeaderTerm)
        state.stateData.leaderMember should be(Option(currentLeaderMemberIndex))
      }

      currentLeaderTerm = newLeaderTerm()
      currentLeaderMemberIndex = leaderMemberIndex
      // This InstallSnapshot will be processed since it indicates newer snapshots than the first command
      follower ! InstallSnapshot(
        shardId,
        term = currentLeaderTerm,
        srcMemberIndex = currentLeaderMemberIndex,
        srcLatestSnapshotLastLogTerm = leaderSnapshotTerm2,
        srcLatestSnapshotLastLogLogIndex = leaderSnapshotLogIndex2,
      )
      LoggingTestKit.info("Snapshot synchronization completed").expect {
        snapshotStore.receiveWhile(messages = 2) {
          case msg: SaveSnapshot =>
            msg.replyTo ! SaveSnapshotSuccess(msg.snapshot.metadata)
        } should have length 2
      }
      inside(region.expectMsgType[ReplicationRegion.DeliverTo]) {
        case ReplicationRegion.DeliverTo(`leaderMemberIndex`, reply: InstallSnapshotSucceeded) =>
          reply.shardId should be(shardId)
          reply.term should be(currentLeaderTerm)
          reply.dstLatestSnapshotLastLogLogIndex should be(leaderSnapshotLogIndex2)
          reply.sender should be(followerMemberIndex)
      }
    }
  }
}
