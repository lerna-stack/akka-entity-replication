package lerna.akka.entityreplication

import akka.Done
import akka.actor.{ Actor, ActorRef, Identify, Props, Terminated }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import akka.pattern.ask
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeSpec, MultiNodeSpecCallbacks }
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.{ DefaultTimeout, ImplicitSender }
import lerna.akka.entityreplication.STMultiNodeSpec.TestActorAutoKillManager
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike }

object STMultiNodeSpec {

  object TestActorAutoKillManager {
    def props = Props(new TestActorAutoKillManager)

    final case class Register(ref: ActorRef)
    final case object KillAll
  }

  class TestActorAutoKillManager extends Actor {
    import TestActorAutoKillManager._

    var refs: Set[ActorRef] = Set()

    override def receive: Receive = ready

    def ready: Receive = {
      case Terminated(ref) =>
        refs -= ref
      case Register(ref) =>
        refs += context.watch(ref)
      case KillAll if refs.isEmpty =>
        sender() ! Done
      case KillAll =>
        refs.foreach(context.stop)
        context.become(terminating(replyTo = sender()))
    }

    def terminating(replyTo: ActorRef): Receive = {
      case Terminated(ref) =>
        refs -= ref
        if (refs.isEmpty) {
          replyTo ! Done
          context.become(ready)
        }
    }
  }
}

/**
  * Multi Node Testing • Akka Documentation
  * https://doc.akka.io/docs/akka/2.5/multi-node-testing.html#a-multi-node-testing-example
  */
trait STMultiNodeSpec
    extends MultiNodeSpecCallbacks
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ImplicitSender
    with DefaultTimeout { self: MultiNodeSpec =>

  override def initialParticipants: Int = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  /**
    * テストケース終了時に登録されたアクターを自動 kill します
    *
    * テストケース内で作成したアクターをケース終了時に一括で kill させたいときに便利です。
    */
  private[this] lazy val autoKillManager: ActorRef =
    system.actorOf(TestActorAutoKillManager.props, "TestActorAutoKillManager")

  protected def planAutoKill(ref: ActorRef): ActorRef = {
    autoKillManager ! TestActorAutoKillManager.Register(ref)
    ref
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    (autoKillManager ? Identify("to wait for start-up")).await
    enterBarrier("starting test")
  }

  override def afterEach(): Unit = {
    enterBarrier("a test case finished")
    (autoKillManager ? TestActorAutoKillManager.KillAll).await
    super.afterEach()
  }

  def isolate(roleName: RoleName): Unit = {
    enterBarrier(s"isolate $roleName")
    runOn(roles.head) {
      println(s"=== isolate $roleName ===")
      roles.filterNot(_ == roleName).foreach(testConductor.blackhole(roleName, _, Direction.Both).await)
    }
    enterBarrier(s"$roleName isolation complete")
  }

  def releaseIsolation(roleName: RoleName): Unit = {
    enterBarrier(s"releaseIsolation $roleName")
    runOn(roles.head) {
      println(s"=== releaseIsolation $roleName ===")
      roles.filterNot(_ == roleName).foreach(testConductor.passThrough(roleName, _, Direction.Both).await)
    }
    enterBarrier(s"$roleName release isolation complete")
  }

  def awaitClusterUp(): Unit = {
    Cluster(system).subscribe(testActor, classOf[MemberUp])
    expectMsgType[CurrentClusterState]

    Cluster(system).join(node(roles.head).address)

    receiveN(roles.size).map {
      case MemberUp(member) => member.address
    }.toSet should be(roles.map(node(_).address).toSet)

    Cluster(system).unsubscribe(testActor)
    enterBarrier("started up cluster members")
  }
}
