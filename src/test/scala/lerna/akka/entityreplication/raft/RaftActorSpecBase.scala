package lerna.akka.entityreplication.raft

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ Actor, ActorRef, Props }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.RaftTestProbe._
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex, Term }
import lerna.akka.entityreplication.raft.protocol.RaftCommands.AppendEntries
import lerna.akka.entityreplication.raft.routing.MemberIndex

trait RaftActorSpecBase extends ActorSpec { self: TestKit =>

  import RaftActor._

  protected val config: Config = system.settings.config
  protected val defaultRaftSettings: RaftSettings = RaftSettings(
    ConfigFactory
      .parseString("""
    // electionTimeout がテスト中に自動で発生しないようにする
    lerna.akka.entityreplication.raft.election-timeout = 999999s
    """).withFallback(config),
  )

  type RaftTestFSMRef = ActorRef

  protected def createRaftActor(
      shardId: NormalizedShardId = NormalizedShardId.from("test"),
      shardSnapshotStore: ActorRef = TestProbe().ref,
      region: ActorRef = TestProbe().ref,
      selfMemberIndex: MemberIndex = MemberIndex("test-index"),
      otherMemberIndexes: Set[MemberIndex] = Set(),
      settings: RaftSettings = defaultRaftSettings,
      replicationActor: ActorRef = Actor.noSender,
  ): RaftTestFSMRef = {
    val replicationActorProps = Props(new Actor() {
      override def receive: Receive = {
        case message => replicationActor forward message
      }
    })
    val shardSnapshotStoreProps = Props(new Actor {
      override def receive: Receive = {
        case msg => shardSnapshotStore forward msg
      }
    })
    val extractEntityId: PartialFunction[ReplicationRegion.Msg, (NormalizedEntityId, ReplicationRegion.Msg)] = {
      case msg => (NormalizedEntityId.from("dummy"), msg)
    }
    val ref = system.actorOf(
      Props(
        new RaftActor(
          typeName = "dummy",
          shardId,
          extractEntityId = extractEntityId,
          replicationActorProps,
          region,
          shardSnapshotStoreProps = shardSnapshotStoreProps,
          selfMemberIndex,
          otherMemberIndexes,
          settings,
          maybeCommitLogStore = None,
        ) with RaftTestProbeSupport,
      ),
    )
    awaitCond(getState(ref).stateName == Follower)
    planAutoKill(ref)
  }

  protected def setState(raftActor: ActorRef, state: State, data: RaftMemberData): Unit = {
    raftActor.tell(SetState(state, data), testActor)
    expectMsg(StateChanged)
  }

  private[this] val getStateProbe = TestProbe()

  protected def getState(raftActor: ActorRef): RaftState = {
    raftActor.tell(GetState, getStateProbe.ref)
    getStateProbe.expectMsgType[RaftState]
  }

  protected def createAppendEntries(
      shardId: NormalizedShardId,
      term: Term,
      leader: MemberIndex,
      prevLogIndex: LogEntryIndex = LogEntryIndex.initial(),
      prevLogTerm: Term = Term.initial(),
      entries: Seq[LogEntry] = Seq(),
      leaderCommit: LogEntryIndex = LogEntryIndex.initial(),
  ): AppendEntries =
    AppendEntries(shardId, term, leader, prevLogIndex, prevLogTerm, entries, leaderCommit)

  private[this] val memberIndexGen = new AtomicInteger(0)

  protected def createUniqueMemberIndex(): MemberIndex = MemberIndex(s"member-${memberIndexGen.incrementAndGet()}")

  private[this] val shardIdGen = new AtomicInteger(0)

  protected def createUniqueShardId(): NormalizedShardId =
    NormalizedShardId.from(s"shard-${shardIdGen.incrementAndGet()}")
}
