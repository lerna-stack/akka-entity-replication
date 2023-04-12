package lerna.akka.entityreplication.raft

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ Actor, ActorRef, Props }
import akka.persistence.testkit.{ PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ReplicationRegion
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.RaftTestProbe._
import lerna.akka.entityreplication.raft.model.{ LogEntry, LogEntryIndex, Term }
import lerna.akka.entityreplication.raft.protocol.RaftCommands.AppendEntries
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.util.ActorIds

object RaftActorSpecBase {

  /** Returns a config to use PersistenceTestKitPlugin and PersistenceTestKitSnapshotPlugin */
  def configWithPersistenceTestKits: Config = {
    PersistenceTestKitPlugin.config
      .withFallback(PersistenceTestKitSnapshotPlugin.config)
      .withFallback(raftPersistenceConfigWithPersistenceTestKits)
      .withFallback(ConfigFactory.load())
  }

  private val raftPersistenceConfigWithPersistenceTestKits: Config = ConfigFactory.parseString(
    s"""
       |lerna.akka.entityreplication.raft.persistence {
       |  journal.plugin = ${PersistenceTestKitPlugin.PluginId}
       |  snapshot-store.plugin = ${PersistenceTestKitSnapshotPlugin.PluginId}
       |  # Might be possible to use PersistenceTestKitReadJournal
       |  // query.plugin = ""
       |}
       |""".stripMargin,
  )

  val defaultRaftPersistenceConfig: Config = ConfigFactory.parseString(
    s"""
       |lerna.akka.entityreplication.raft.persistence {
       |  journal.plugin = "inmemory-journal"
       |  snapshot-store.plugin = "inmemory-snapshot-store"
       |  query.plugin = "inmemory-read-journal"
       |}
       |""".stripMargin,
  )

}

trait RaftActorSpecBase extends ActorSpec { self: TestKit =>

  import RaftActor._

  protected val config: Config = system.settings.config
  protected val defaultRaftConfig: Config = ConfigFactory
    .parseString("""
    // electionTimeout がテスト中に自動で発生しないようにする
    lerna.akka.entityreplication.raft.election-timeout = 999999s
    """).withFallback(config)

  type RaftTestFSMRef = ActorRef

  protected val defaultTypeName: TypeName           = TypeName.from("dummy")
  protected val defaultShardId: NormalizedShardId   = NormalizedShardId.from("test")
  protected val defaultSelfMemberIndex: MemberIndex = MemberIndex("test-index")

  protected def raftActorPersistenceId(
      typeName: TypeName = defaultTypeName,
      shardId: NormalizedShardId = defaultShardId,
      selfMemberIndex: MemberIndex,
  ): String =
    ActorIds.persistenceId("raft", typeName.underlying, shardId.underlying, selfMemberIndex.role)

  protected def createRaftActor(
      shardId: NormalizedShardId = defaultShardId,
      shardSnapshotStore: ActorRef = TestProbe().ref,
      region: ActorRef = TestProbe().ref,
      selfMemberIndex: MemberIndex = defaultSelfMemberIndex,
      otherMemberIndexes: Set[MemberIndex] = Set(),
      settings: RaftSettings = RaftSettings(defaultRaftConfig),
      replicationActor: ActorRef = system.deadLetters,
      typeName: TypeName = defaultTypeName,
      entityId: NormalizedEntityId = NormalizedEntityId.from("dummy"),
      commitLogStore: ActorRef = TestProbe().ref,
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
    createRaftActorWithProps(
      shardId,
      shardSnapshotStoreProps,
      region,
      selfMemberIndex,
      otherMemberIndexes,
      settings,
      replicationActorProps,
      typeName,
      entityId,
      commitLogStore,
    )
  }

  protected def createRaftActorWithProps(
      shardId: NormalizedShardId = defaultShardId,
      shardSnapshotStoreProps: Props = Props.empty,
      region: ActorRef = TestProbe().ref,
      selfMemberIndex: MemberIndex = defaultSelfMemberIndex,
      otherMemberIndexes: Set[MemberIndex] = Set(),
      settings: RaftSettings = RaftSettings(defaultRaftConfig),
      replicationActorProps: Props = Props.empty,
      typeName: TypeName = defaultTypeName,
      entityId: NormalizedEntityId = NormalizedEntityId.from("dummy"),
      commitLogStore: ActorRef = TestProbe().ref,
  ): RaftTestFSMRef = {
    val extractEntityId: PartialFunction[ReplicationRegion.Msg, (NormalizedEntityId, ReplicationRegion.Msg)] = {
      case msg => (entityId, msg)
    }
    val ref = system.actorOf(
      Props(
        new RaftActor(
          typeName,
          extractEntityId = extractEntityId,
          _ => replicationActorProps,
          region,
          shardSnapshotStoreProps = shardSnapshotStoreProps,
          selfMemberIndex,
          otherMemberIndexes,
          settings,
          commitLogStore,
        ) with RaftTestProbeSupport,
      ),
      name = shardId.underlying,
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
