package lerna.akka.entityreplication.raft

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.{ typed, ActorRef, ActorSystem }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model._
import lerna.akka.entityreplication.raft.RaftActor._
import lerna.akka.entityreplication.raft.RaftProtocol._
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.protocol.RaftCommands._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol._
import org.scalactic.TypeCheckedTripleEquals

final class RaftActorPersistenceDeletionSpec
    extends TestKit(
      ActorSystem("RaftActorPersistenceDeletionSpec", RaftActorSpecBase.configWithPersistenceTestKits),
    )
    with RaftActorSpecBase
    with TypeCheckedTripleEquals {

  private implicit val typedSystem: typed.ActorSystem[Nothing] = system.toTyped
  private val persistenceTestKit                               = PersistenceTestKit(system)
  private val snapshotTestKit                                  = SnapshotTestKit(system)

  override def beforeEach(): Unit = {
    super.beforeEach()
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
  }

  private def configFor(
      deleteBeforeRelativeSequenceNr: Long,
      deleteOldEvents: Boolean = false,
      deleteOldSnapshots: Boolean = false,
  ): Config = {
    ConfigFactory
      .parseString(
        s"""
           |lerna.akka.entityreplication.raft {
           |  election-timeout = 99999s
           |  compaction.log-size-threshold = 3
           |  compaction.preserve-log-size = 1
           |  compaction.log-size-check-interval = 99999s
           |  delete-old-events = $deleteOldEvents
           |  delete-old-snapshots = $deleteOldSnapshots
           |  delete-before-relative-sequence-nr = $deleteBeforeRelativeSequenceNr
           |}
           |""".stripMargin,
      ).withFallback(system.settings.config)
  }

  private trait Fixture {
    val shardId: NormalizedShardId     = createUniqueShardId()
    val selfIndex: MemberIndex         = createUniqueMemberIndex()
    val otherIndexA: MemberIndex       = createUniqueMemberIndex()
    val otherIndexB: MemberIndex       = createUniqueMemberIndex()
    val otherIndices: Set[MemberIndex] = Set(otherIndexA, otherIndexB)

    val persistenceId: String =
      raftActorPersistenceId(defaultTypeName, shardId, selfIndex)
    val entityId: NormalizedEntityId =
      NormalizedEntityId.from("entity")

    val otherRaftActor: TestProbe = TestProbe()

    val replicationRegion: TestProbe = TestProbe()
    val replicationActor: TestProbe  = TestProbe()
    val snapshotStore: TestProbe     = TestProbe()
    val commitLogStore: TestProbe    = TestProbe()

    // The following actors ignore all messages by default since they can receive messages periodically in the background.
    // Disable ignore when it has to verify incoming messages.
    replicationRegion.ignoreMsg { case _ => true }
    replicationActor.ignoreMsg { case _ => true }
    snapshotStore.ignoreMsg { case _ => true }
    commitLogStore.ignoreMsg { case _ => true }

    def createFollower(config: Config): ActorRef = {
      val settings = RaftSettings(config)
      val followerData = {
        val initialEntries = Seq(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
        )
        val replicatedLog = ReplicatedLog().truncateAndAppend(initialEntries)
        RaftMemberData(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(1),
          lastApplied = LogEntryIndex(1),
          eventSourcingIndex = Option(LogEntryIndex(1)),
        ).initializeFollowerData()
      }
      val raftActor = createRaftActor(
        shardId = shardId,
        shardSnapshotStore = snapshotStore.ref,
        region = replicationRegion.ref,
        selfMemberIndex = selfIndex,
        otherMemberIndexes = otherIndices,
        settings = settings,
        replicationActor = replicationActor.ref,
        entityId = entityId,
        commitLogStore = commitLogStore.ref,
      )
      setState(raftActor, Follower, followerData)
      raftActor
    }

    def advanceCompaction(raftActor: ActorRef, newEventSourcingIndex: LogEntryIndex): Unit = {
      commitLogStore.ignoreNoMsg()
      replicationActor.ignoreNoMsg()
      snapshotStore.ignoreNoMsg()

      // Trigger compaction
      raftActor ! SnapshotTick

      // Advance EventSourcingIndex
      commitLogStore.fishForSpecificMessage() {
        case _: CommitLogStoreActor.AppendCommittedEntries =>
          commitLogStore.reply(CommitLogStoreActor.AppendCommittedEntriesResponse(newEventSourcingIndex))
      }

      // Advance EntitySnapshot taking
      replicationActor.fishForSpecificMessage() {
        case msg: TakeSnapshot =>
          replicationActor.reply(Snapshot(msg.metadata, EntityState("state")))
      }

      // Advance EntitySnapshot saving
      snapshotStore.fishForSpecificMessage() {
        case msg: SaveSnapshot =>
          snapshotStore.reply(SaveSnapshotSuccess(msg.snapshot.metadata))
      }

      commitLogStore.ignoreMsg { case _ => true }
      replicationActor.ignoreMsg { case _ => true }
      snapshotStore.ignoreMsg { case _ => true }
    }

    def appendNewEntriesTo(
        follower: ActorRef,
        currentTerm: Term,
        prevLogTerm: Term,
        firstEntry: LogEntry,
        entries: LogEntry*,
    ): Unit = {
      val newEntries   = firstEntry +: entries
      val prevLogIndex = firstEntry.index.prev()
      val lastLogIndex = entries.lastOption.getOrElse(firstEntry).index
      otherRaftActor.send(
        follower,
        AppendEntries(shardId, currentTerm, otherIndexA, prevLogIndex, prevLogTerm, newEntries, lastLogIndex),
      )
      otherRaftActor.expectMsg(AppendEntriesSucceeded(currentTerm, lastLogIndex, selfIndex))
    }

    def appendNewEntriesEachTo(
        follower: ActorRef,
        currentTerm: Term,
        prevLogTerm: Term,
        firstEntry: LogEntry,
        entries: LogEntry*,
    ): Unit = {
      appendNewEntriesTo(follower, currentTerm, prevLogTerm, firstEntry)
      for ((entry, prevEntry) <- entries.zip(firstEntry +: entries)) {
        appendNewEntriesTo(follower, currentTerm, prevEntry.term, entry)
      }
    }

    def assertRaftActorHandlesNewMessage(raftActor: ActorRef): Unit = {
      // Use RequestVote since all RaftActor states can handle RequestVote.
      otherRaftActor.send(
        raftActor,
        RequestVote(shardId, Term(1), otherIndexA, LogEntryIndex(1), Term(1)),
      )
      otherRaftActor.expectMsgType[RequestVoteDenied]
    }

  }

  "Follower" should {
    def promoteToFollower(fixture: Fixture, follower: ActorRef): Unit = { /* Do nothing */ }

    behave like eventDeletionSupport(promoteBeforeDelete = promoteToFollower)
    behave like snapshotDeletionSupport(promoteBeforeDelete = promoteToFollower)
  }

  "Candidate" should {
    def promoteToCandidate(fixture: Fixture, follower: ActorRef): Unit = {
      follower ! ElectionTimeout
      awaitAssert {
        assert(getState(follower).stateName === Candidate)
      }
    }

    behave like eventDeletionSupport(promoteBeforeDelete = promoteToCandidate)
    behave like snapshotDeletionSupport(promoteBeforeDelete = promoteToCandidate)
  }

  "Leader" should {
    def promoteToLeader(fixture: Fixture, follower: ActorRef): Unit = {
      import fixture._
      replicationRegion.ignoreNoMsg()
      follower ! ElectionTimeout
      replicationRegion.fishForSpecificMessage() {
        case ReplicationRegion.Broadcast(requestVote: RequestVote) =>
          replicationRegion.forward(replicationRegion.lastSender, requestVote)
          replicationRegion.reply(RequestVoteAccepted(requestVote.term, otherIndexA))
          replicationRegion.reply(RequestVoteAccepted(requestVote.term, otherIndexB))
      }
      replicationRegion.ignoreMsg { case _ => true }
      awaitAssert {
        assert(getState(follower).stateName === Leader)
      }
    }

    behave like eventDeletionSupport(promoteBeforeDelete = promoteToLeader)
    behave like snapshotDeletionSupport(promoteBeforeDelete = promoteToLeader)
  }

  def eventDeletionSupport(promoteBeforeDelete: (Fixture, ActorRef) => Unit): Unit = {

    "not delete events if event deletion is disabled" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 0,
          deleteOldEvents = false,
        ),
      )

      // Arrange: append new entries, which will trigger compaction.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-3"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // Arrange: store the number of events to verify event deletion.
      val numOfEventsBeforeDelete =
        persistenceTestKit.persistedInStorage(persistenceId).size

      // Act: complete compaction, which will trigger a snapshot save and event deletion.
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(4))

      // Assert:
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)
      assertForDuration(
        {
          assert(persistenceTestKit.persistedInStorage(persistenceId).size >= numOfEventsBeforeDelete)
        },
        max = remainingOrDefault,
      )
    }

    "delete events matching the criteria if it successfully saves a snapshot" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 1,
          deleteOldEvents = true,
        ),
      )

      // Arrange: append new entries, which will trigger compaction.
      appendNewEntriesEachTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-3"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // Arrange: store the number of events to verify event deletion.
      val numOfEventsBeforeDelete =
        persistenceTestKit.persistedInStorage(persistenceId).size

      // Act: complete compaction, which will trigger a snapshot save and event deletion.
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(4))

      // Assert:
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)
      awaitAssert {
        assert(persistenceTestKit.persistedInStorage(persistenceId).size < numOfEventsBeforeDelete)
        assert(persistenceTestKit.persistedInStorage(persistenceId).size === 1)
      }
    }

    "not delete events if it successfully saves a snapshot, but no events match the criteria" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 1000,
          deleteOldEvents = true,
        ),
      )

      // Arrange: append new entries, which will trigger compaction.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-3"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // Arrange: store the number of events to verify event deletion.
      val numOfEventsBeforeDelete =
        persistenceTestKit.persistedInStorage(persistenceId).size

      // Act: complete compaction, which will trigger a snapshot save and event deletion.
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(4))

      // Assert:
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)
      assertForDuration(
        {
          assert(persistenceTestKit.persistedInStorage(persistenceId).size >= numOfEventsBeforeDelete)
        },
        max = remainingOrDefault,
      )
    }

    "continue its behavior if an event deletion fails" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 1,
          deleteOldEvents = true,
        ),
      )

      // Arrange: append new entries, which will trigger compaction.
      appendNewEntriesEachTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-3"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // Arrange: store the number of events to verify event deletion.
      val numOfEventsBeforeDelete =
        persistenceTestKit.persistedInStorage(persistenceId).size

      // Act: complete compaction, which will trigger a snapshot save and event deletion.
      LoggingTestKit.warn("Failed to deleteMessages").expect {
        persistenceTestKit.failNextDelete(persistenceId)
        advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(4))
        snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)
      }

      // Assert:
      awaitAssert {
        assert(persistenceTestKit.persistedInStorage(persistenceId).size >= numOfEventsBeforeDelete)
        assert(persistenceTestKit.persistedInStorage(persistenceId).size > 1)
      }

      // Assert: RaftActor should handle the next message.
      assertRaftActorHandlesNewMessage(raftActor)
    }
  }

  def snapshotDeletionSupport(promoteBeforeDelete: (Fixture, ActorRef) => Unit): Unit = {

    "not delete snapshots if snapshot deletion is disabled" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 0,
          deleteOldSnapshots = false,
        ),
      )

      // Arrange: complete the first compaction, which will trigger a snapshot save.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
      )
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(3))
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)

      // Arrange: append new entries, which will trigger the second compaction.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-3"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event-4"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event-5"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // Act: complete the second compaction, which will trigger a snapshot save and deletion.
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(6))

      // Assert:
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)
      assertForDuration(
        {
          assert(snapshotTestKit.persistedInStorage(persistenceId).size === 2)
        },
        max = remainingOrDefault,
      )
    }

    "delete snapshots matching the criteria if it successfully saves a snapshot" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 0,
          deleteOldSnapshots = true,
        ),
      )

      // Arrange: complete the first compaction, which will trigger a snapshot save.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
      )
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(3))
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)

      // Arrange: append new entries, which will trigger the second compaction.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-b-3"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event-b-4"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event-b-5"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // NOTE:
      //   Assert that a new snapshot is saved successfully by inspecting an info log since
      //   snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId) might not work
      //   in this case.
      LoggingTestKit.info("Succeeded to saveSnapshot").expect {
        // Act: completes the second compaction, which will trigger a snapshot save and deletion.
        advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(6))
      }

      // Assert:
      awaitAssert {
        assert(snapshotTestKit.persistedInStorage(persistenceId).size === 1)
      }
    }

    "not delete snapshots if it successfully saves a snapshot, but no snapshots match the criteria" in new Fixture {
      val raftActor = createFollower(
        configFor(
          deleteBeforeRelativeSequenceNr = 1000,
          deleteOldSnapshots = true,
        ),
      )

      // Arrange: complete the first compaction, which will trigger a snapshot save.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event-1"), Term(1)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event-2"), Term(1)),
      )
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(3))
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)

      // Arrange: append new entries, which will trigger the second compaction.
      appendNewEntriesTo(
        raftActor,
        currentTerm = Term(1),
        prevLogTerm = Term(1),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event-3"), Term(1)),
        LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event-4"), Term(1)),
        LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event-5"), Term(1)),
      )

      promoteBeforeDelete(this, raftActor)

      // Act: complete the second compaction, which will trigger a snapshot save and deletion.
      advanceCompaction(raftActor, newEventSourcingIndex = LogEntryIndex(6))

      // Assert:
      snapshotTestKit.expectNextPersistedType[PersistentStateData.PersistentState](persistenceId)
      assertForDuration(
        {
          assert(snapshotTestKit.persistedInStorage(persistenceId).size === 2)
        },
        max = remainingOrDefault,
      )
    }

    "continue its behavior if a snapshot deletion fails" ignore {
      // TODO: Write this test after `SnapshotTestKit.failNextDelete` comes to trigger a snapshot deletion failure.
      //   `SnapshotTestKit.failNextDelete` doesn't trigger a snapshot deletion failure at the time of writing.
      //   While underlying implementation `PersistenceTestKitSnapshotPlugin.deleteAsync` is supposed to return a failed
      //   Future, it always returns a successful Future.
    }

  }

}
