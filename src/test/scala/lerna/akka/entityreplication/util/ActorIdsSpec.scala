package lerna.akka.entityreplication.util

import java.net.URLEncoder

import org.scalatest.{ Matchers, WordSpec }

class ActorIdsSpec extends WordSpec with Matchers {

  private[this] def urlEncode(value: String) = URLEncoder.encode(value, "utf-8")

  "ActorIds" when {

    "creates actorName" should {

      "return a name for an actor" in {
        ActorIds.actorName("test", "actor") should be("test:actor")
      }

      "be passed encoded elements with URLEncoder if they can contain the delimiter" in {
        ActorIds.actorName(urlEncode("test:a"), urlEncode("actor:b")) should be("test%3Aa:actor%3Ab")
      }

      "throw an exception when it gets invalid element which contains the delimiter" in {
        val exception = intercept[IllegalArgumentException] {
          ActorIds.actorName("test:a", "actor:b")
        }
        exception.getMessage should be("requirement failed: Not URL encoded value found: (1: test:a), (2: actor:b)")
      }
    }

    "creates persistenceId" should {

      "return an id for a PersistentActor" in {
        ActorIds.persistenceId("test", "actor") should be("test:actor")
      }

      "be passed encoded elements with URLEncoder if they can contain the delimiter" in {
        ActorIds.persistenceId(urlEncode("test:a"), urlEncode("actor:b")) should be("test%3Aa:actor%3Ab")
      }

      "throw an exception when it gets invalid element which contains the delimiter" in {
        val exception = intercept[IllegalArgumentException] {
          ActorIds.persistenceId("test:a", "actor:b")
        }
        exception.getMessage should be("requirement failed: Not URL encoded value found: (1: test:a), (2: actor:b)")
      }
    }
  }
}
