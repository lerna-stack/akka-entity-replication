package lerna.akka.entityreplication.testkit

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.raft.ActorSpec

class CustomTestProbeSpec extends TestKit(ActorSystem("CustomTestProbeSpec")) with ActorSpec {
  import CustomTestProbe._

  "CustomTestProbe.fishMatchMessagesWhile" should {

    "pass when the probe receives messages that match the condition" in {
      val probe = TestProbe()

      probe.ref ! "match"

      var called = false
      probe.fishMatchMessagesWhile(messages = 1) {
        case "match" =>
          called = true
      }
      called should be(true)
    }

    "throw AssertionError when the probe doesn't receive any messages that match the condition" in {
      val probe = TestProbe()

      probe.ref ! "invalid"

      intercept[AssertionError] {
        probe.fishMatchMessagesWhile(messages = 1) {
          case "match" =>
        }
      }
    }

    "ignore messages that doesn't match the condition" in {
      val probe = TestProbe()

      probe.ref ! "ignore"
      probe.ref ! "ignore"
      probe.ref ! "match"

      var called = false
      probe.fishMatchMessagesWhile(messages = 1) {
        case "match" =>
          called = true
      }
      called should be(true)
    }

    "call the function until the count of matched messages reaches 'messages' parameter" in {
      val probe = TestProbe()

      probe.ref ! "match"
      probe.ref ! "match"

      var count = 0
      probe.fishMatchMessagesWhile(messages = 2) {
        case "match" =>
          count = count + 1
      }
      count should be(2)
    }
  }
}
