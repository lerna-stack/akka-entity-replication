package lerna.akka.entityreplication.typed

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, TestProbe }
import akka.actor.typed.{ ActorRef, RecipientRef, Scheduler }
import akka.pattern.StatusReply
import akka.util.Timeout
import lerna.akka.entityreplication.typed.internal.ReplicatedEntityRefImpl
import org.scalatest.{ Inside, Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures

import java.util.concurrent.TimeoutException
import scala.concurrent.Future
import scala.concurrent.duration._

object ReplicatedEntityRefSpec {

  sealed trait Command

  final case class TellMessage()                                              extends Command
  final case class AskMessage(replyTo: ActorRef[Done])                        extends Command
  final case class AskWithStatusMessage(replyTo: ActorRef[StatusReply[Done]]) extends Command
}

class ReplicatedEntityRefSpec extends WordSpec with Matchers with Inside with ScalaFutures {

  import ReplicatedEntityRefSpec._

  private[this] val testkit = ActorTestKit()

  private[this] val typeKey = ReplicatedEntityTypeKey[Command]("test-entity-type-key")

  private[this] val entityId = "test-entity"

  private[this] val replicationRegion: TestProbe[ReplicationEnvelope[Command]] =
    testkit.createTestProbe("Region")

  private[this] def createEntityRef(): ReplicatedEntityRef[Command] = {
    new ReplicatedEntityRefImpl[Command](
      typeKey = typeKey,
      entityId = entityId,
      replicationRegion = replicationRegion.ref,
      system = testkit.system,
    )
  }

  "ReplicatedEntityRef" should {

    "tell a message wrapped in ReplicationEnvelope to ReplicationRegion" in {
      val entityRef = createEntityRef()

      entityRef ! TellMessage()

      inside(replicationRegion.expectMessageType[ReplicationEnvelope[Command]]) {
        case ReplicationEnvelope(envelopeEntityId, message) =>
          envelopeEntityId should be(entityId)
          message should be(TellMessage())
      }
    }

    "ask a message that requires reply wrapped in ReplicationEnvelope to ReplicationRegion" in {
      val entityRef                 = createEntityRef()
      implicit val timeout: Timeout = Timeout(3.seconds)

      val reply: Future[Done] = entityRef ? AskMessage

      inside(replicationRegion.expectMessageType[ReplicationEnvelope[Command]]) {
        case ReplicationEnvelope(envelopeEntityId, AskMessage(replyTo)) =>
          envelopeEntityId should be(entityId)
          replyTo ! Done
      }

      reply.futureValue should be(Done)
    }

    "be a RecipientRef to use ask pattern APIs" in {
      import akka.actor.typed.scaladsl.AskPattern._
      val entityRef: RecipientRef[Command] = createEntityRef()
      implicit val timeout: Timeout        = Timeout(3.seconds)
      implicit val scheduler: Scheduler    = testkit.scheduler

      val reply: Future[Done] = entityRef ? AskMessage

      inside(replicationRegion.expectMessageType[ReplicationEnvelope[Command]]) {
        case ReplicationEnvelope(envelopeEntityId, AskMessage(replyTo)) =>
          envelopeEntityId should be(entityId)
          replyTo ! Done
      }

      reply.futureValue should be(Done)
    }

    "time out if there is no reply to the ask" in {
      val entityRef                 = createEntityRef()
      implicit val timeout: Timeout = Timeout(10.millis)

      val reply: Future[Done] = entityRef ? AskMessage

      inside(replicationRegion.expectMessageType[ReplicationEnvelope[Command]]) {
        case ReplicationEnvelope(envelopeEntityId, AskMessage(_)) =>
          envelopeEntityId should be(entityId)
        // no reply
      }

      reply.failed.futureValue shouldBe a[TimeoutException]
    }

    "ask a message that requires reply with StatusReply wrapped in ReplicationEnvelope to ReplicationRegion" in {
      val entityRef                 = createEntityRef()
      implicit val timeout: Timeout = Timeout(3.seconds)

      val reply: Future[Done] = entityRef askWithStatus AskWithStatusMessage

      inside(replicationRegion.expectMessageType[ReplicationEnvelope[Command]]) {
        case ReplicationEnvelope(envelopeEntityId, AskWithStatusMessage(replyTo)) =>
          envelopeEntityId should be(entityId)
          replyTo ! StatusReply.Success(Done)
      }

      reply.futureValue should be(Done)
    }

    "time out if there is no reply to the askWithStatus" in {
      val entityRef                 = createEntityRef()
      implicit val timeout: Timeout = Timeout(10.millis)

      val reply: Future[Done] = entityRef askWithStatus AskWithStatusMessage

      inside(replicationRegion.expectMessageType[ReplicationEnvelope[Command]]) {
        case ReplicationEnvelope(envelopeEntityId, AskWithStatusMessage(_)) =>
          envelopeEntityId should be(entityId)
        // no reply
      }

      reply.failed.futureValue shouldBe a[TimeoutException]
    }
  }
}
