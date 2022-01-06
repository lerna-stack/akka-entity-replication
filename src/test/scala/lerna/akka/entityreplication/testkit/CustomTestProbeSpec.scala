package lerna.akka.entityreplication.testkit

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.raft.ActorSpec

class CustomTestProbeSpec extends TestKit(ActorSystem("CustomTestProbeSpec")) with ActorSpec {
  import CustomTestProbe._

  "CustomTestProbe.fishForMessageN" should {

    "pass when the probe receives messages that match the condition" in {
      val probe = TestProbe()

      probe.ref ! "match"

      var called = false
      probe.fishForMessageN(messages = 1) {
        case "match" =>
          called = true
      }
      called should be(true)
    }

    "throw AssertionError when the probe doesn't receive any messages that match the condition" in {
      val probe = TestProbe()

      probe.ref ! "invalid"

      intercept[AssertionError] {
        probe.fishForMessageN(messages = 1) {
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
      probe.fishForMessageN(messages = 1) {
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
      probe.fishForMessageN(messages = 2) {
        case "match" =>
          count = count + 1
      }
      count should be(2)
    }

    "return the values the PartialFunction provided to verify coverage of the patterns matched" in {
      val probe = TestProbe()

      probe.ref ! "first"
      probe.ref ! "second"

      probe.fishForMessageN(messages = 2) {
        case msg @ "first" =>
          msg
        case msg @ "second" =>
          msg
      } should contain theSameElementsInOrderAs Seq("first", "second")
    }
  }
}
