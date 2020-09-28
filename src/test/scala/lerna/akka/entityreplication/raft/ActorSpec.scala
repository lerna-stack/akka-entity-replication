package lerna.akka.entityreplication.raft

import akka.Done
import akka.actor.{ Actor, ActorRef, Identify, Props, Terminated }
import akka.pattern.ask
import akka.testkit.{ DefaultTimeout, ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpecLike }

import scala.concurrent.{ Await, Awaitable }

object ActorSpec {

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

trait ActorSpec extends WordSpecLike with Matchers with BeforeAndAfterEach with ImplicitSender with DefaultTimeout {
  self: TestKit =>
  import ActorSpec._

  /**
    * (ported from akka.remote.testkit.MultiNodeSpec)
    *
    * Enrich `.await()` onto all Awaitables, using remaining duration from the innermost
    * enclosing `within` block or QueryTimeout.
    */
  implicit class AwaitHelper[T](w: Awaitable[T]) {
    def await: T = Await.result(w, timeout.duration)
  }

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
  }

  override def afterEach(): Unit = {
    (autoKillManager ? TestActorAutoKillManager.KillAll).await
    super.afterEach()
  }
}
