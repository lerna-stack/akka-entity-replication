package lerna.akka.entityreplication

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ Actor, ActorRef, Props }
import akka.cluster.ClusterEvent.{ InitialStateAsEvents, MemberUp }
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import com.typesafe.config.ConfigFactory
import lerna.akka.entityreplication.model.{ NormalizedEntityId, NormalizedShardId }
import lerna.akka.entityreplication.raft.RaftProtocol.{ ReplicationSucceeded, _ }
import lerna.akka.entityreplication.raft.RaftTestProbe._
import lerna.akka.entityreplication.raft.model._
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.{ RaftActor, RaftMemberData, RaftMemberDataImpl, RaftSettings }
import org.scalatest.Inside.inside

import scala.concurrent.duration._

object RaftActorSpecConfig extends MultiNodeConfig {
  val controller = role("controller")
  val node1      = role("node1")
  val node2      = role("node2")
  val node3      = role("node3")

  testTransport(true)

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      lerna.akka.entityreplication.raft.multi-raft-roles = ["member-1", "member-2", "member-3"]
      """))
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-1"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-2"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString("""
    akka.cluster.roles = ["member-3"]
  """))
}

class RaftActorSpecMultiJvmController extends RaftActorSpec
class RaftActorSpecMultiJvmNode1      extends RaftActorSpec
class RaftActorSpecMultiJvmNode2      extends RaftActorSpec
class RaftActorSpecMultiJvmNode3      extends RaftActorSpec

class RaftActorSpec extends MultiNodeSpec(RaftActorSpecConfig) with STMultiNodeSpec {

  import RaftActor._
  import RaftActorSpecConfig._

  private[this] val config = system.settings.config
  private[this] val defaultRaftSettings = new {
    // テスト中はデフォルトで各種タイマーが作動しないようにする
    override val electionTimeout   = 999.seconds
    override val heartbeatInterval = 99.seconds
  } with RaftSettings(config)

  private[this] val entityId = NormalizedEntityId.from("test-entity")

  type RaftTestFSMRef = ActorRef

  "RaftActor" should {

    "wait for all nodes to enter a barrier" in {

      cluster.subscribe(testActor, InitialStateAsEvents, classOf[MemberUp])

      cluster.join(node(node1).address)

      receiveN(roles.size).map {
        case MemberUp(member) => member.address
      }.toSet should be(roles.map(node(_).address).toSet)

      cluster.unsubscribe(testActor)

      enterBarrier("started up a cluster")
    }

    "最初は全てのメンバーが Follower になる" in {

      val shardId = createSeqShardId()

      runOn(node1) {
        val raftMember1 = createRaftActor(shardId)
        awaitAssert(getState(raftMember1).stateName should be(Follower))
      }
      runOn(node2) {
        val raftMember2 = createRaftActor(shardId)
        awaitAssert(getState(raftMember2).stateName should be(Follower))
      }
      runOn(node3) {
        val raftMember3 = createRaftActor(shardId)
        awaitAssert(getState(raftMember3).stateName should be(Follower))
      }
    }

    "ElectionTimeout をきっかけに選挙によってメンバーの中から唯一の Leader が選出される" in {

      val shardId = createSeqShardId()

      var followerMember: RaftTestFSMRef = null
      var leaderMember: RaftTestFSMRef   = null
      runOn(node1) {
        followerMember = createRaftActor(
          shardId,
          new {
            // 確実にリーダーになるように仕向けるため
            override val electionTimeout = 99.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(followerMember).stateName == Follower)
      }
      runOn(node2) {
        followerMember = createRaftActor(
          shardId,
          new {
            // 確実にリーダーになるように仕向けるため
            override val electionTimeout = 99.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(followerMember).stateName == Follower)
      }
      runOn(node3) {
        leaderMember = createRaftActor(
          shardId,
          new {
            // 確実にリーダーになるように仕向けるため
            override val electionTimeout = 1.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(leaderMember).stateName == Leader)
      }
      enterBarrier("Leader elected")
      val expectedLeaderMemberIndex = MemberIndex("member-3")
      runOn(node1, node2) {
        awaitAssert {
          val state = getState(followerMember)
          state.stateName should be(Follower)
          state.stateData.currentTerm should be > Term.initial()
          state.stateData.votedFor should contain(expectedLeaderMemberIndex)
        }
      }
      runOn(node3) {
        val state = getState(leaderMember)
        state.stateName should be(Leader)
        state.stateData.currentTerm should be > Term.initial()
        state.stateData.votedFor should contain(expectedLeaderMemberIndex)
      }
    }

    "Candidate が同時に現れても選挙を再試行して Leader が選出される" in {

      val replicationId = createSeqShardId()

      val term                            = Term.initial()
      var candidateMember: RaftTestFSMRef = null
      runOn(node1) {
        candidateMember = createRaftActor(
          replicationId,
          new {
            override val electionTimeout = 6.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(candidateMember).stateName == Follower)
        setState(candidateMember, Candidate, createCandidateData(term))
      }
      runOn(node2) {
        candidateMember = createRaftActor(
          replicationId,
          new {
            override val electionTimeout = 6.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(candidateMember).stateName == Follower)
        setState(candidateMember, Candidate, createCandidateData(term))
      }
      runOn(node3) {
        candidateMember = createRaftActor(
          replicationId,
          new {
            // 確実にリーダーになるように仕向けるため
            override val electionTimeout = 3.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(candidateMember).stateName == Follower)
        setState(candidateMember, Candidate, createCandidateData(term))
      }
      runOn(node3) {
        // 2回目以降のタイムアウトは node3 だけ短いので node3 がリーダーになるはず
        awaitAssert(getState(candidateMember).stateName should be(Leader), max = 15.seconds)
      }
    }

    "Replicate コマンドによってイベントがレプリケーションされる" in {
      val replicationId = createSeqShardId()

      var leaderMember: RaftTestFSMRef   = null
      var followerMember: RaftTestFSMRef = null
      runOn(node1) {
        leaderMember = createRaftActor(
          replicationId,
          new {
            // リーダーとして選出させるため
            override val electionTimeout   = 500.millis
            override val heartbeatInterval = 100.millis
          } with RaftSettings(config),
        )
        awaitCond(getState(leaderMember).stateName == Leader)
      }
      runOn(node2) {
        followerMember = createRaftActor(replicationId)
        awaitCond(getState(followerMember).stateName == Follower)
      }
      runOn(node3) {
        followerMember = createRaftActor(replicationId)
        awaitCond(getState(followerMember).stateName == Follower)
      }
      enterBarrier("raft member up")

      val dummyEvent = "dummyEvent"

      runOn(node1) {
        leaderMember ! Replicate(dummyEvent, testActor, entityId)
        inside(expectMsgType[ReplicationSucceeded]) {
          case ReplicationSucceeded(event, _) => event should be(dummyEvent)
        }
        getState(leaderMember).stateData.replicatedLog.last.event.event should be(dummyEvent)
      }
      enterBarrier("event replicated")

      runOn(node2) {
        awaitAssert(getState(followerMember).stateData.replicatedLog.last.event.event should be(dummyEvent))
      }
      runOn(node3) {
        awaitAssert(getState(followerMember).stateData.replicatedLog.last.event.event should be(dummyEvent))
      }
    }

    "ReplicationSucceeded が返ってきたらコミットされたとみなせる" in {
      val replicationId = createSeqShardId()

      var leaderMember: RaftTestFSMRef   = null
      var followerMember: RaftTestFSMRef = null
      runOn(node1) {
        leaderMember = createRaftActor(
          replicationId,
          new {
            // リーダーとして選出させるため
            override val electionTimeout   = 500.millis
            override val heartbeatInterval = 100.millis
          } with RaftSettings(config),
        )
        awaitCond(getState(leaderMember).stateName == Leader)
      }
      runOn(node2) {
        followerMember = createRaftActor(replicationId)
        awaitCond(getState(followerMember).stateName == Follower)
      }
      runOn(node3) {
        followerMember = createRaftActor(replicationId)
        awaitCond(getState(followerMember).stateName == Follower)
      }
      enterBarrier("raft member up")

      val dummyEvent = "dummyEvent"

      // LogEntryIndex(1) は no-op
      val expectedCommitIndex = LogEntryIndex(2)

      runOn(node1) {
        leaderMember ! Replicate(dummyEvent, testActor, entityId)
        inside(expectMsgType[ReplicationSucceeded]) {
          case ReplicationSucceeded(event, _) => event should be(dummyEvent)
        }
        getState(leaderMember).stateData.commitIndex should be(expectedCommitIndex)
      }
      enterBarrier("event replicated")

      // リーダーがコミットしてから遅れて（Heartbeat をきっかけに） Follower もコミットする
      runOn(node2) {
        awaitAssert(getState(followerMember).stateData.commitIndex should be(expectedCommitIndex))
      }
      runOn(node3) {
        awaitAssert(getState(followerMember).stateData.commitIndex should be(expectedCommitIndex))
      }
    }

    "Follower のログの prevLogIndex の Term が prevLogTerm と一致しない場合はログ同期を再試行する" in {
      val replicationId = createSeqShardId()

      var leaderMember: RaftTestFSMRef   = null
      var followerMember: RaftTestFSMRef = null
      runOn(node1) {
        leaderMember = createRaftActor(
          replicationId,
          new {
            // リーダーとして選出させるため
            override val electionTimeout   = 3.seconds
            override val heartbeatInterval = 1.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(leaderMember).stateName == Leader)
        val leaderData = getState(leaderMember).stateData
        val log = leaderData.replicatedLog.merge(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "correct1"), Term(2)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "correct2"), Term(2)),
            LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "correct3"), Term(2)),
          ),
          LogEntryIndex.initial(),
        )
        setState(leaderMember, Leader, leaderData.asInstanceOf[RaftMemberDataImpl].copy(replicatedLog = log))
      }
      runOn(node2) {
        followerMember = createRaftActor(replicationId)
        awaitCond(getState(followerMember).stateName == Follower)
        val followerData = getState(followerMember).stateData
        val conflictLog = followerData.replicatedLog.merge(
          Seq(
            LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "conflict1"), Term(1)),
            LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "conflict2"), Term(1)),
          ),
          LogEntryIndex.initial(),
        )
        setState(
          followerMember,
          Follower,
          followerData.asInstanceOf[RaftMemberDataImpl].copy(replicatedLog = conflictLog),
        )
      }
      runOn(node3) {
        followerMember = createRaftActor(replicationId)
        awaitCond(getState(followerMember).stateName == Follower)
      }
      enterBarrier("raft member up")

      val dummyEvent = "dummyEvent"

      val expectedLog = Seq(
        LogEntry(LogEntryIndex(1), EntityEvent(Option(entityId), "correct1"), Term(2)),
        LogEntry(LogEntryIndex(2), EntityEvent(Option(entityId), "correct2"), Term(2)),
        LogEntry(LogEntryIndex(3), EntityEvent(Option(entityId), "correct3"), Term(2)),
        LogEntry(LogEntryIndex(4), EntityEvent(Option(entityId), dummyEvent), Term(1)),
      )

      runOn(node1) {
        leaderMember ! Replicate(dummyEvent, testActor, entityId)
        inside(expectMsgType[ReplicationSucceeded]) {
          case ReplicationSucceeded(event, _) => event should be(dummyEvent)
        }
        getState(leaderMember).stateData.replicatedLog.entries should contain theSameElementsInOrderAs expectedLog
      }
      enterBarrier("event replicated")

      runOn(node2) {
        awaitAssert(
          getState(followerMember).stateData.replicatedLog.entries should contain theSameElementsInOrderAs expectedLog,
        )
      }
      runOn(node3) {
        awaitAssert(
          getState(followerMember).stateData.replicatedLog.entries should contain theSameElementsInOrderAs expectedLog,
        )
      }
    }

    "will eventually be the only leader if it has the most recent log entry even if multiple leader was elected" in {
      // Scenario
      // *: is a leader  -: belongs with isolated network
      // (1)
      //  * node1 Term:1 Log:[(Term(1), NoOp), (Term(1), event1)]
      //    node2 Term:1 Log:[(Term(1), NoOp)]
      //    node3 Term:1 Log:[(Term(1), NoOp)]
      // (2)
      // -* node1 Term:1 Log:[(Term(1), NoOp), (Term(1), event1)]
      //    node2 Term:2 Log:[(Term(1), NoOp), (Term(2), NoOp)]
      // -* node3 Term:2 Log:[(Term(1), NoOp), (Term(2), NoOp), (Term(2), event2)]
      // (3)
      //  * node1 Term:3 Log:[(Term(1), NoOp), (Term(1), event1)]
      //    node2 Term:3 Log:[(Term(1), NoOp), (Term(2), NoOp),   (Term(2), event2)] <- replicated in the majority (committed)
      //  * node3 Term:2 Log:[(Term(1), NoOp), (Term(2), NoOp),   (Term(2), event2)]
      // (4)
      //    node1 Term:3 Log:[(Term(1), NoOp), (Term(2), NoOp),   (Term(2), event2)] <- becomes a follower (uncommitted entries will be overwritten)
      //    node2 Term:3 Log:[(Term(1), NoOp), (Term(2), NoOp),   (Term(2), event2)]
      //  * node3 Term:3 Log:[(Term(1), NoOp), (Term(2), NoOp),   (Term(2), event2)]

      val replicationId = createSeqShardId()

      var nodeMember: RaftTestFSMRef = null
      // make node1 be a leader
      runOn(node1) {
        nodeMember = createRaftActor(
          replicationId,
          new {
            // to make it be a leader
            override val electionTimeout   = 1.seconds
            override val heartbeatInterval = 0.5.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(nodeMember).stateName == Leader)
      }
      runOn(node2) {
        nodeMember = createRaftActor(replicationId)
        awaitCond(getState(nodeMember).stateName == Follower)
      }
      runOn(node3) {
        nodeMember = createRaftActor(
          replicationId,
          new {
            // to make it be a leader
            override val electionTimeout   = 6.seconds
            override val heartbeatInterval = 0.5.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(nodeMember).stateName == Follower)
      }
      runOn(node1, node2, node3) {
        awaitCond(
          getState(nodeMember).stateData.replicatedLog.nonEmpty && getState(
            nodeMember,
          ).stateData.replicatedLog.last.event.event == NoOp,
        )
      }
      enterBarrier("a leader is elected")

      // Scenario (1)
      // to prevent events are replicated
      isolate(node1, excludes = Set(controller))
      runOn(node1) {
        nodeMember ! Replicate("event1", testActor, entityId)
        awaitCond(getState(nodeMember).stateData.replicatedLog.entries.exists(_.event.event == "event1"))
      }
      enterBarrier("complete scenario (1)")

      // Scenario (2)
      runOn(node3) {
        // node 1 が孤立するため
        awaitCond(getState(nodeMember).stateName == Leader)
      }
      runOn(node2) {
        awaitCond {
          getState(nodeMember).stateData.replicatedLog.lastOption.exists { e =>
            e.term > Term(1) && e.event.event == NoOp
          }
        }
      }
      enterBarrier("scenario (2): node3 becomes a leader")

      // replicates event2 to only node3
      isolate(node3, excludes = Set(controller))
      runOn(node3) {
        nodeMember ! Replicate("event2", testActor, entityId)
        awaitCond(getState(nodeMember).stateData.replicatedLog.entries.exists(_.event.event == "event2"))
      }
      enterBarrier("complete scenario (2)")

      // Scenario (3)
      // resolves the network isolation in the situation as node1 and node3 are leader together
      releaseIsolation(node1)
      releaseIsolation(node3)

      // waits until the event is replicated to majority
      runOn(node3) {
        inside(expectMsgType[ReplicationSucceeded]) {
          case ReplicationSucceeded(event, _) => event should be("event2")
        }
      }
      runOn(node2) {
        awaitCond {
          // event2 was committed
          val commitIndex = getState(nodeMember).stateData.commitIndex
          getState(nodeMember).stateData.replicatedLog
            .sliceEntries(LogEntryIndex.initial(), commitIndex).exists(_.event.event == "event2")
        }
      }
      enterBarrier("complete scenario (3)")

      // Scenario (4)
      runOn(node1) {
        awaitAssert {
          val replicatedLog = getState(nodeMember).stateData.replicatedLog
          replicatedLog.entries.map(_.event.event) should contain("event2")
          replicatedLog.entries.map(_.event.event) should not contain "event1"
        }
      }
      enterBarrier("complete scenario (4)")
    }

    "メンバー全てがシャットダウンしても再作成すると状態が復元する" in {
      val replicationId = createSeqShardId()

      var raftMember: RaftTestFSMRef = null
      runOn(node1) {
        raftMember = createRaftActor(
          replicationId,
          new {
            // リーダーになりやすくするため
            override val electionTimeout   = 1.seconds
            override val heartbeatInterval = 0.5.seconds
          } with RaftSettings(config),
        )
        awaitCond(getState(raftMember).stateName == Leader)
      }
      runOn(node2, node3) {
        raftMember = createRaftActor(replicationId)
        awaitCond(getState(raftMember).stateName == Follower)
      }
      enterBarrier("raft member up")

      val dummyEvent = "dummyEvent"

      runOn(node1) {
        raftMember ! Replicate(dummyEvent, testActor, entityId)
        inside(expectMsgType[ReplicationSucceeded]) {
          case ReplicationSucceeded(event, _) => event should be(dummyEvent)
        }
      }
      enterBarrier("sent event")
      var dataBeforeCrash: RaftMemberData = null
      runOn(node1, node2, node3) {
        awaitCond(getState(raftMember).stateData.replicatedLog.last.event.event == dummyEvent)
        dataBeforeCrash = getState(raftMember).stateData
      }
      enterBarrier("event replicated")

      runOn(node1, node2, node3) {
        // メンバー全てをシャットダウン
        watch(raftMember)
        // Region ごと停止する
        system.stop(system.actorSelection(raftMember.path.parent).resolveOne().await)
        expectTerminated(raftMember)
        // 再作成
        raftMember = createRaftActor(replicationId)
        awaitAssert {
          // 内部状態が復元
          val currentData = getState(raftMember).stateData
          currentData.votedFor should be(dataBeforeCrash.votedFor)
          currentData.currentTerm should be(dataBeforeCrash.currentTerm)
          currentData.replicatedLog.entries should contain theSameElementsInOrderAs dataBeforeCrash.replicatedLog.entries
        }
      }
    }
  }

  private[this] def createRaftActor(
      shardId: NormalizedShardId,
      settings: RaftSettings = defaultRaftSettings,
  ): RaftTestFSMRef = {
    val replicationActorProps = Props(new Actor() {
      override def receive: Receive = Actor.emptyBehavior
    })
    val extractEntityId: ReplicationRegion.ExtractEntityId = { case msg => ("test-entity", msg) }
    val extractShardId: ReplicationRegion.ExtractShardId = { _ => shardId.underlying }
    val typeName = s"sample-${shardId.underlying}"
    val regionProps = {
      Props(
        new ReplicationRegion(
          typeName = typeName,
          replicationActorProps,
          ClusterReplicationSettings(system),
          extractEntityId,
          extractShardId,
          maybeCommitLogStore = None,
        ) {
          override def createRaftActorProps(): Props = {
            Props(
              new RaftActor(
                typeName = "test",
                extractNormalizedEntityId,
                replicationActorProps,
                self,
                shardSnapshotStoreProps = Props.empty,
                selfMemberIndex,
                otherMemberIndexes,
                settings,
                maybeCommitLogStore = None,
              ) with RaftTestProbeSupport,
            )
          }
        },
      )
    }
    val regionRef = planAutoKill(system.actorOf(regionProps, s"ReplicationRegion-for-${shardId.underlying}"))
    regionRef ! "create RaftActor"

    def resolveRaftActor(): ActorRef = {
      val role         = "*" // Specify a wildcard because the role differs depending on the node
      val clusterShard = "*" // Since cluster shard is calculated from hash and difficult to predict, specify a wildcard
      system
        .actorSelection(
          s"/system/sharding/raft-shard-$typeName-$role/$clusterShard/${shardId.underlying}",
        ).resolveOne(100.millis).await
    }

    awaitAssert(resolveRaftActor())
  }

  private[this] def createCandidateData(
      currentTerm: Term,
      log: ReplicatedLog = ReplicatedLog(),
      commitIndex: LogEntryIndex = LogEntryIndex.initial(),
      acceptedMembers: Set[MemberIndex] = Set(),
      members: Set[RaftMember] = Set(),
  ): RaftMemberData =
    RaftMemberData(currentTerm, replicatedLog = log, commitIndex = commitIndex, acceptedMembers = acceptedMembers)
      .initializeCandidateData()

  private[this] val idGenerator = new AtomicInteger(0)
  private[this] def createSeqShardId(): NormalizedShardId =
    NormalizedShardId.from(s"replication-${idGenerator.incrementAndGet()}")

  protected def setState(raftActor: ActorRef, state: State, data: RaftMemberData): Unit = {
    raftActor ! SetState(state, data)
    expectMsg(max = 10.seconds, StateChanged)
  }

  protected def getState(raftActor: ActorRef): RaftState = {
    raftActor ! GetState
    expectMsgType[RaftState]
  }
}
