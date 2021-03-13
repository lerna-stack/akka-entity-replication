package lerna.akka.entityreplication.testkit

import akka.actor.{ ActorSystem, PoisonPill, Props }
import akka.testkit.{ ImplicitSender, TestKit }
import lerna.akka.entityreplication.{ ReplicationActor, ReplicationRegion }
import lerna.akka.entityreplication.testkit.TestReplicationActorPropsSpec.WordCountReplicationActor
import org.scalatest.{ Matchers, WordSpecLike }

object TestReplicationActorPropsSpec {

  object WordCountReplicationActor {
    def props: Props = Props(new WordCountReplicationActor)

    sealed trait Command
    final case class CountWord(text: String) extends Command
    final case class GetCount()              extends Command
    final case class Stop()                  extends Command

    final case class CurrentCount(count: Int)

    sealed trait DomainEvent
    final case class Counted(wordCount: Int) extends DomainEvent
  }

  class WordCountReplicationActor extends ReplicationActor[Int] {
    import WordCountReplicationActor._

    private[this] var count = 0

    override def receiveReplica: Receive = {
      case domainEvent: DomainEvent => updateState(domainEvent)
    }

    override def receiveCommand: Receive = {
      case CountWord(text) =>
        replicate(Counted(wordCount = text.length)) { event =>
          updateState(event)
          sender() ! event
        }
      case GetCount() =>
        ensureConsistency {
          sender() ! CurrentCount(count)
        }
      case Stop() =>
        context.parent ! ReplicationRegion.Passivate(self.path, PoisonPill)
    }

    private[this] def updateState(event: DomainEvent): Unit =
      event match {
        case Counted(wordCount) => count = count + wordCount
      }

    override def currentState: Int = count
  }
}

class TestReplicationActorPropsSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with ImplicitSender {
  import WordCountReplicationActor._

  "TestReplicationActorProps" should {

    "take in only ReplicationActor Props" in {
      val ex = intercept[IllegalArgumentException] {
        TestReplicationActorProps(Props.default)
      }
      ex.getMessage should be(s"The Props for [${Props.default.actorClass()}] doesn't provide ReplicationActor")
    }

    "forward commands to ReplicationActor" in {
      val actor = system.actorOf(TestReplicationActorProps(WordCountReplicationActor.props))
      actor ! CountWord("hello")
      expectMsg(Counted(wordCount = "hello".length))
    }

    "allow that ReplicationActor process ensureConsistency" in {
      val actor = system.actorOf(TestReplicationActorProps(WordCountReplicationActor.props))
      actor ! CountWord("hello")
      expectMsgType[Counted]
      actor ! GetCount()
      expectMsg(CurrentCount("hello".length))
    }

    "allow to check ReplicationActor passivation" in {
      val actor = watch(system.actorOf(TestReplicationActorProps(WordCountReplicationActor.props)))
      actor ! Stop() // Passivate

      // TestReplicationActor terminates self after ReplicationActor terminated
      expectTerminated(actor)
    }
  }

}
