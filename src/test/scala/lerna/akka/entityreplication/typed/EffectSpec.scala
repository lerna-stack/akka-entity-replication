package lerna.akka.entityreplication.typed

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.{ FlatSpec, Matchers }

object EffectSpec {
  final case object Event
  final case object Reply
  trait State
}

class EffectSpec extends FlatSpec with Matchers {

  import EffectSpec._

  private[this] val actorTestKit = ActorTestKit()

  private[this] val replyTo = actorTestKit.createTestProbe[Reply.type]()

  "Effect when replicate() is called" should "return the event" in {
    val effect = Effect.replicate(Event).thenNoReply()
    effect.event should contain(Event)
  }

  "Effect when none is called" should behave like anEffectHasNoEvent {
    Effect.none.thenReply(replyTo.ref)(_ => Reply)
  }
  "Effect when unhandled is called" should behave like anEffectHasNoEvent {
    Effect.unhandled.thenReply(replyTo.ref)(_ => Reply)
  }
  "Effect when passivate() is called" should behave like anEffectHasNoEvent {
    Effect.passivate().thenNoReply()
  }
  "Effect when stopLocally() is called" should behave like anEffectHasNoEvent {
    Effect.stopLocally()
  }
  "Effect when unstashAll() is called" should behave like anEffectHasNoEvent {
    Effect.unstashAll().thenNoReply()
  }
  "Effect when stash() is called" should behave like anEffectHasNoEvent {
    Effect.stash()
  }
  "Effect when reply() is called" should behave like anEffectHasNoEvent {
    Effect.reply(replyTo.ref)(Reply)
  }
  "Effect when noReply is called" should behave like anEffectHasNoEvent {
    Effect.noReply
  }

  def anEffectHasNoEvent(effect: => Effect[Event.type, State]): Unit = {
    it should s"return None from event method" in {
      effect.event should be(empty)
    }
  }
}
