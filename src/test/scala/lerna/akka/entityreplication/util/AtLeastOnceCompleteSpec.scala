package lerna.akka.entityreplication.util

import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, FishingOutcomes }
import akka.actor.typed.ActorRef

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ typed, ActorSystem, NoSerializationVerificationNeeded, Status }
import akka.pattern.{ AskTimeoutException, StatusReply }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.util.AtLeastOnceCompleteSpec._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._

object AtLeastOnceCompleteSpec {
  private val config: Config = ConfigFactory
    .parseString(s"""
                    | akka.actor {
                    |   provider = local
                    | }
       """.stripMargin)
    .withFallback(ConfigFactory.load())

  final case class RequestMessage(message: String) extends NoSerializationVerificationNeeded
  object RequestMessage {
    private val counter = new AtomicInteger()

    def unique() = new RequestMessage(counter.getAndIncrement().toString)
  }

  final case class ResponseMessage(message: String) extends NoSerializationVerificationNeeded
  object ResponseMessage {
    private val counter = new AtomicInteger()

    def unique() = new ResponseMessage(counter.getAndIncrement().toString)
  }

  final case class RequestMessageTyped(message: String, replyTo: ActorRef[ResponseMessage])
      extends NoSerializationVerificationNeeded
  object RequestMessageTyped {
    private val counter = new AtomicInteger()

    def uniqueId(): String = counter.getAndIncrement().toString
  }

  final case class RequestMessageRequiresStatus(message: String, replyTo: ActorRef[StatusReply[ResponseMessage]])
      extends NoSerializationVerificationNeeded
  object RequestMessageRequiresStatus {
    private val counter = new AtomicInteger()

    def uniqueId(): String = counter.getAndIncrement().toString
  }
}

class AtLeastOnceCompleteSpec
    extends TestKit(ActorSystem("AtLeastOnceCompleteSpec", AtLeastOnceCompleteSpec.config))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override def afterAll(): Unit = {
    super.afterAll()
    shutdown()
  }

  "AtLeastOnceComplete.askTo" must {
    "deliver message to destination" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askTo(destination, message, retryInterval)

      destinationProbe.expectMsg(message)
    }

    "return the response wrapped in Future" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val response                      = ResponseMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      val future = AtLeastOnceComplete.askTo(destination, request, retryInterval)

      destinationProbe.expectMsg(request)
      destinationProbe.reply(response)

      whenReady(future) {
        _ shouldBe response
      }
    }

    "return Future.failed when Status.Failure is returned from destination" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val responseError                 = new RuntimeException("dummy")
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      val future = AtLeastOnceComplete.askTo(destination, request, retryInterval)

      destinationProbe.expectMsg(request)
      destinationProbe.reply(Status.Failure(responseError))

      whenReady(future.failed) {
        _ shouldBe responseError
      }
    }

    "resend the message to destination if there is no response within the retryInterval time" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val response                      = ResponseMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askTo(destination, request, retryInterval)

      destinationProbe.expectMsg(request)
      destinationProbe.expectMsg(request)
      destinationProbe.expectMsg(request)
      destinationProbe.reply(response)
    }

    "return Future.failed if there is no response by timeout" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(50.milliseconds)

      val future = AtLeastOnceComplete.askTo(destination, request, retryInterval)

      whenReady(future.failed) {
        _ shouldBe a[AskTimeoutException]
      }
    }

    "stop resending when a response is returned" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val response                      = ResponseMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askTo(destination, request, retryInterval)

      destinationProbe.expectMsg(request)
      destinationProbe.reply(response)

      destinationProbe.expectNoMessage()
    }

    "stop resending when Status.Failure is returned" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askTo(destination, request, retryInterval)

      destinationProbe.expectMsg(request)
      destinationProbe.reply(Status.Failure(new RuntimeException))

      destinationProbe.expectNoMessage()
    }

    "stop resending after timeout" in {
      val destinationProbe              = TestProbe()
      val destination                   = destinationProbe.ref
      val request                       = RequestMessage.unique()
      val retryInterval: FiniteDuration = 200.milliseconds
      implicit val timeout: Timeout     = Timeout(900.milliseconds)

      val future = AtLeastOnceComplete.askTo(destination, request, retryInterval)

      destinationProbe.receiveWhile(timeout.duration) {
        case `request` => // ok
      }

      whenReady(future.failed) { _ => }

      destinationProbe.expectNoMessage()
    }
  }

  import akka.actor.typed.scaladsl.adapter._
  private[this] val typedActorTestKit = ActorTestKit(system.toTyped)

  implicit val typedSystem: typed.ActorSystem[_] = typedActorTestKit.internalSystem

  "AtLeastOnceComplete.askTo (Typed)" must {

    "deliver a message to the destination and return the response wrapped in Future" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageTyped]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageTyped.uniqueId()
      val response                      = ResponseMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      val future =
        AtLeastOnceComplete.askTo(destination, RequestMessageTyped(message, _), retryInterval)

      val request = destinationProbe.receiveMessage()
      request.message should be(message)
      request.replyTo ! response

      future.futureValue should be(response)
    }

    "resend the message to the destination if there is no response within the retryInterval time" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageTyped]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageTyped.uniqueId()
      val response                      = ResponseMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askTo(destination, RequestMessageTyped(message, _), retryInterval)

      val responses =
        destinationProbe.receiveMessages(3)
      responses.map(_.message) should contain only message
      responses.last.replyTo ! response
    }

    "return Future.failed if there is no response by timeout" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageTyped]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageTyped.uniqueId()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(50.milliseconds)

      val future = AtLeastOnceComplete.askTo(destination, RequestMessageTyped(message, _), retryInterval)

      future.failed.futureValue shouldBe a[TimeoutException]
    }

    "stop resending when a response is returned" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageTyped]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageTyped.uniqueId()
      val response                      = ResponseMessage.unique()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askTo(destination, RequestMessageTyped(message, _), retryInterval)

      val request = destinationProbe.receiveMessage()
      request.replyTo ! response

      destinationProbe.expectNoMessage()
    }

    "stop resending after timeout" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageTyped]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageTyped.uniqueId()
      val retryInterval: FiniteDuration = 200.milliseconds
      implicit val timeout: Timeout     = Timeout(900.milliseconds)

      val future =
        AtLeastOnceComplete.askTo(destination, RequestMessageTyped(message, _), retryInterval)

      // wait for a timeout
      intercept[AssertionError] {
        // ignore all message until timeout expired
        destinationProbe.fishForMessage(timeout.duration)(_ => FishingOutcomes.continueAndIgnore)
      }
      future.failed.futureValue shouldBe a[TimeoutException]

      destinationProbe.expectNoMessage()
    }
  }

  "AtLeastOnceComplete.askWithStatusTo (Typed)" must {

    "return Future.failed when Status.Failure is returned from destination" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageRequiresStatus]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageRequiresStatus.uniqueId()
      val responseError                 = new RuntimeException("dummy")
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      val future =
        AtLeastOnceComplete.askWithStatusTo(destination, RequestMessageRequiresStatus(message, _), retryInterval)

      val request = destinationProbe.receiveMessage()
      request.message should be(message)
      request.replyTo ! StatusReply.Error(responseError)

      future.failed.futureValue should be(responseError)
    }

    "stop resending when Status.Failure is returned" in {
      val destinationProbe              = typedActorTestKit.createTestProbe[RequestMessageRequiresStatus]()
      val destination                   = destinationProbe.ref
      val message                       = RequestMessageRequiresStatus.uniqueId()
      val retryInterval: FiniteDuration = 10.milliseconds
      implicit val timeout: Timeout     = Timeout(1000.milliseconds)

      AtLeastOnceComplete.askWithStatusTo(destination, RequestMessageRequiresStatus(message, _), retryInterval)

      val request = destinationProbe.receiveMessage()
      request.replyTo ! StatusReply.Error(new RuntimeException)

      destinationProbe.expectNoMessage()
    }
  }

}
