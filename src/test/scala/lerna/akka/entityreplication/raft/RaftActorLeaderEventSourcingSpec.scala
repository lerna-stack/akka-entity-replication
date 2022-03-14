package lerna.akka.entityreplication.raft

import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.{ actor => classic }
import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor
import lerna.akka.entityreplication.raft.model.{ EntityEvent, LogEntry, LogEntryIndex, NoOp, ReplicatedLog, Term }
import org.scalatest.Inside

final class RaftActorLeaderEventSourcingSpec extends TestKit(classic.ActorSystem()) with RaftActorSpecBase with Inside {

  import RaftActor._
  import eventsourced.CommitLogStoreActor._
  import lerna.akka.entityreplication.raft.RaftActorLeaderEventSourcingSpec._

  private implicit val typedSystem: ActorSystem[Nothing] = system.toTyped

  private val raftConfig: Config =
    ConfigFactory
      .parseString("""
        |# EventSourcingTick will be emitted manually by each test case.
        |# Use enough larger value to avoid emitting such the tick by timers.
        |lerna.akka.entityreplication.raft.eventsourced.committed-log-entries-check-interval = 1000s
        |""".stripMargin).withFallback(defaultRaftConfig)

  private def spawnLeader(
      currentTerm: Term,
      replicatedLog: ReplicatedLog,
      commitIndex: LogEntryIndex,
      lastApplied: LogEntryIndex,
      eventSourcingIndex: Option[LogEntryIndex],
      shardId: NormalizedShardId = createUniqueShardId(),
      raftSettings: RaftSettings = RaftSettings(raftConfig),
      commitLogStore: ActorRef = TestProbe().ref,
  ): RaftTestFSMRef = {
    val leader = createRaftActor(
      shardId = shardId,
      selfMemberIndex = createUniqueMemberIndex(),
      settings = raftSettings,
      commitLogStore = commitLogStore,
    )
    val leaderData = createLeaderData(
      currentTerm = currentTerm,
      replicatedLog = replicatedLog,
      commitIndex = commitIndex,
      lastApplied = lastApplied,
      eventSourcingIndex = eventSourcingIndex,
    )
    setState(leader, Leader, leaderData)
    leader
  }

  "Leader" should {

    "handle AppendCommittedEntriesResponse(index=0) and update its eventSourcingIndex when it has no eventSourcingIndex" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val leader = spawnLeader(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = None,
      )
      leader ! AppendCommittedEntriesResponse(LogEntryIndex(0))
      getState(leader).stateData.eventSourcingIndex should be(Some(LogEntryIndex(0)))
    }

    "handle AppendCommittedEntriesResponse(index=1) and update its eventSourcingIndex when it has no eventSourcingIndex" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val leader = spawnLeader(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = None,
      )
      leader ! AppendCommittedEntriesResponse(LogEntryIndex(1))
      getState(leader).stateData.eventSourcingIndex should be(Some(LogEntryIndex(1)))
    }

    "handle AppendCommittedEntriesResponse(index=2) and update its eventSourcingIndex when its eventSourcingIndex is 1" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val leader = spawnLeader(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = Some(LogEntryIndex(1)),
      )
      leader ! AppendCommittedEntriesResponse(LogEntryIndex(2))
      getState(leader).stateData.eventSourcingIndex should be(Some(LogEntryIndex(2)))
    }

    "handle AppendCommittedEntriesResponse(index=2) and update nothing when its eventSourcingIndex is 2" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val leader = spawnLeader(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = Some(LogEntryIndex(2)),
      )
      leader ! AppendCommittedEntriesResponse(LogEntryIndex(2))
      getState(leader).stateData.eventSourcingIndex should be(Some(LogEntryIndex(2)))
    }

    "handle AppendCommittedEntriesResponse(index=1) and update nothing when its eventSourcingIndex is 2" in {
      val replicatedLog: ReplicatedLog = {
        val entityId = NormalizedEntityId.from("entity1")
        newReplicatedLog(
          LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
        )
      }
      val leader = spawnLeader(
        currentTerm = Term(1),
        replicatedLog = replicatedLog,
        commitIndex = LogEntryIndex(2),
        lastApplied = LogEntryIndex(2),
        eventSourcingIndex = Some(LogEntryIndex(2)),
      )
      leader ! AppendCommittedEntriesResponse(LogEntryIndex(1))
      getState(leader).stateData.eventSourcingIndex should be(Some(LogEntryIndex(2)))
    }

    "handle EventSourcingTick and send AppendCommittedEntries(entries=empty) when it has no eventSourcingIndex" in {
      val shardId        = createUniqueShardId()
      val commitLogStore = TestProbe()
      val leader = {
        val replicatedLog = {
          val entityId = NormalizedEntityId.from("entity1")
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          )
        }
        spawnLeader(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(2),
          lastApplied = LogEntryIndex(2),
          eventSourcingIndex = None,
          shardId = shardId,
          commitLogStore = commitLogStore.ref,
        )
      }
      leader ! RaftActor.EventSourcingTick
      commitLogStore.expectMsg(
        CommitLogStoreActor.AppendCommittedEntries(shardId, entries = Seq.empty),
      )
    }

    "handle EventSourcingTick, log error, and send AppendCommittedEntries(entries=empty) " +
    "when its committed entries are empty and eventSourcingIndex is less than commitIndex" in {
      val shardId        = createUniqueShardId()
      val commitLogStore = TestProbe()
      val leader = {
        val replicatedLog = {
          val entityId = NormalizedEntityId.from("entity1")
          newReplicatedLog(
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          )
        }
        spawnLeader(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(2),
          lastApplied = LogEntryIndex(2),
          eventSourcingIndex = Some(LogEntryIndex(1)),
          shardId = shardId,
          commitLogStore = commitLogStore.ref,
        )
      }

      LoggingTestKit
        .error(
          "=== [Leader] could not resolve new committed log entries, but there should be. " +
          "nextEventSourcingIndex=[2], commitIndex=[2], foundFirstIndex=[None]. " +
          "This error might happen if compaction deletes such entries before introducing the event-sourcing progress track feature. " +
          s"For confirmation, the leader is sending AppendCommittedEntries(shardId=[$shardId], entries=empty) to fetch the latest eventSourcingIndex.",
        ).expect {
          leader ! RaftActor.EventSourcingTick
          commitLogStore.expectMsg(
            CommitLogStoreActor.AppendCommittedEntries(shardId, entries = Seq.empty),
          )
        }
    }

    "handle EventSourcingTick, log error, and send AppendCommittedEntries(entries=empty) when its entries don't contains the next entry" in {
      val shardId        = createUniqueShardId()
      val commitLogStore = TestProbe()
      val leader = {
        val replicatedLog = {
          val entityId = NormalizedEntityId.from("entity1")
          newReplicatedLog(
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          )
        }
        spawnLeader(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(3),
          lastApplied = LogEntryIndex(3),
          eventSourcingIndex = Some(LogEntryIndex(1)),
          shardId = shardId,
          commitLogStore = commitLogStore.ref,
        )
      }

      LoggingTestKit
        .error(
          "=== [Leader] could not resolve new committed log entries, but there should be. " +
          "nextEventSourcingIndex=[2], commitIndex=[3], foundFirstIndex=[Some(3)]. " +
          "This error might happen if compaction deletes such entries before introducing the event-sourcing progress track feature. " +
          s"For confirmation, the leader is sending AppendCommittedEntries(shardId=[$shardId], entries=empty) to fetch the latest eventSourcingIndex.",
        ).expect {
          leader ! RaftActor.EventSourcingTick
          commitLogStore.expectMsg(
            CommitLogStoreActor.AppendCommittedEntries(shardId, entries = Seq.empty),
          )
        }
    }

    "handle EventSourcingTick and send AppendCommittedEntries(entries=[2,3]) when its entries contains the next entry" in {
      val shardId        = createUniqueShardId()
      val commitLogStore = TestProbe()
      val entityId       = NormalizedEntityId.from("entity1")
      val leader = {
        val replicatedLog = {
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          )
        }
        spawnLeader(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(3),
          lastApplied = LogEntryIndex(3),
          eventSourcingIndex = Some(LogEntryIndex(1)),
          shardId = shardId,
          commitLogStore = commitLogStore.ref,
        )
      }
      leader ! RaftActor.EventSourcingTick
      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          appendCommittedEntries.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          appendCommittedEntries.entries(1),
        )
      }
    }

    "handle EventSourcingTick and send multiple batched AppendCommittedEntries if it has more entries than `max-append-committed-entries-size`" in {
      val shardId        = createUniqueShardId()
      val commitLogStore = TestProbe()
      val entityId       = NormalizedEntityId.from("entity1")
      val raftSettings = RaftSettings(
        ConfigFactory
          .parseString("""
            |lerna.akka.entityreplication.raft.eventsourced.max-append-committed-entries-size = 2
            |lerna.akka.entityreplication.raft.eventsourced.max-append-committed-entries-batch-size = 3
            |""".stripMargin)
          .withFallback(raftConfig),
      )
      val leader = {
        val replicatedLog = {
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
            LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event3"), Term(1)),
            LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event4"), Term(1)),
            LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event5"), Term(1)),
          )
        }
        spawnLeader(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(6),
          lastApplied = LogEntryIndex(6),
          eventSourcingIndex = Some(LogEntryIndex(1)),
          shardId = shardId,
          raftSettings = raftSettings,
          commitLogStore = commitLogStore.ref,
        )
      }
      leader ! RaftActor.EventSourcingTick

      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          appendCommittedEntries.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          appendCommittedEntries.entries(1),
        )
      }

      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event3"), Term(1)),
          appendCommittedEntries.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event4"), Term(1)),
          appendCommittedEntries.entries(1),
        )
      }

      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(1)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event5"), Term(1)),
          appendCommittedEntries.entries(0),
        )
      }

      commitLogStore.expectNoMessage()

    }

    "handle EventSourcingTick and send at most `max-append-committed-entries-batch-size` AppendCommittedEntries" in {
      val shardId        = createUniqueShardId()
      val commitLogStore = TestProbe()
      val entityId       = NormalizedEntityId.from("entity1")
      val raftSettings = RaftSettings(
        ConfigFactory
          .parseString("""
                         |lerna.akka.entityreplication.raft.eventsourced.max-append-committed-entries-size = 2
                         |lerna.akka.entityreplication.raft.eventsourced.max-append-committed-entries-batch-size = 3
                         |""".stripMargin)
          .withFallback(raftConfig),
      )
      val leader = {
        val replicatedLog = {
          newReplicatedLog(
            LogEntry(LogEntryIndex(1), EntityEvent(None, NoOp), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
            LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event3"), Term(1)),
            LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event4"), Term(1)),
            LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event5"), Term(1)),
            LogEntry(LogEntryIndex(7), EntityEvent(Option(entityId), "event6"), Term(1)),
            LogEntry(LogEntryIndex(8), EntityEvent(Option(entityId), "event7"), Term(1)),
          )
        }
        spawnLeader(
          currentTerm = Term(1),
          replicatedLog = replicatedLog,
          commitIndex = LogEntryIndex(8),
          lastApplied = LogEntryIndex(8),
          eventSourcingIndex = Some(LogEntryIndex(1)),
          shardId = shardId,
          raftSettings = raftSettings,
          commitLogStore = commitLogStore.ref,
        )
      }
      leader ! RaftActor.EventSourcingTick

      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "event1"), Term(1)),
          appendCommittedEntries.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "event2"), Term(1)),
          appendCommittedEntries.entries(1),
        )
      }

      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), "event3"), Term(1)),
          appendCommittedEntries.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(5), EntityEvent(Option(entityId), "event4"), Term(1)),
          appendCommittedEntries.entries(1),
        )
      }

      inside(commitLogStore.expectMsgType[CommitLogStoreActor.AppendCommittedEntries]) { appendCommittedEntries =>
        appendCommittedEntries.shardId should be(shardId)
        appendCommittedEntries.entries.size should be(2)
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(6), EntityEvent(Option(entityId), "event5"), Term(1)),
          appendCommittedEntries.entries(0),
        )
        assertEqualsLogEntry(
          LogEntry(LogEntryIndex(7), EntityEvent(Option(entityId), "event6"), Term(1)),
          appendCommittedEntries.entries(1),
        )
      }

      commitLogStore.expectNoMessage()

    }

  }

  private def assertEqualsLogEntry(expected: LogEntry, actual: LogEntry): Unit = {
    actual.term should be(expected.term)
    actual.index should be(expected.index)
    actual.event should be(expected.event)
  }

}

object RaftActorLeaderEventSourcingSpec {

  private def createLeaderData(
      currentTerm: Term,
      replicatedLog: ReplicatedLog,
      commitIndex: LogEntryIndex,
      lastApplied: LogEntryIndex,
      eventSourcingIndex: Option[LogEntryIndex],
  ): RaftMemberData = {
    require(
      currentTerm > Term(0),
      s"Leader should have a term higher than Term(0).",
    )
    require(
      lastApplied <= commitIndex,
      s"lastApplied [$lastApplied] should be less than or equal to commitIndex [$commitIndex]",
    )
    require(
      currentTerm >= replicatedLog.lastLogTerm,
      s"currentTerm [$currentTerm] should be greater than or equal to ReplicatedLog.lastLogTerm [${replicatedLog.lastLogTerm}]",
    )
    RaftMemberData(
      currentTerm = currentTerm,
      replicatedLog = replicatedLog,
      commitIndex = commitIndex,
      lastApplied = lastApplied,
      eventSourcingIndex = eventSourcingIndex,
    ).initializeLeaderData()
  }

  private def newReplicatedLog(
      entries: LogEntry*,
  ): ReplicatedLog = {
    if (entries.isEmpty) {
      ReplicatedLog()
    } else {
      val firstTerm  = entries.head.term
      val firstIndex = entries.head.index
      require(firstTerm > Term(0))
      require(firstIndex > LogEntryIndex(0))
      ReplicatedLog()
        .reset(Term(firstTerm.term - 1), firstIndex.prev())
        .merge(entries, firstIndex.prev())
    }
  }

}
