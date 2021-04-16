package lerna.akka.entityreplication.typed.internal.effect

import akka.actor.typed.ActorRef
import lerna.akka.entityreplication.typed.internal.effect
import lerna.akka.entityreplication.typed.{ Effect, EffectBuilder }

import scala.collection.immutable

object EffectBuilderImpl {

  def unhandled[Event, State](mainEffect: MainEffect[Event, State]): EffectBuilder[Event, State] =
    new EffectBuilderImpl(mainEffect, immutable.Seq(new UnhandledEffect[State]))
}

private[entityreplication] final case class EffectBuilderImpl[+Event, State](
    mainEffect: MainEffect[Event, State],
    sideEffects: immutable.Seq[SideEffect[State]] = immutable.Seq.empty,
) extends EffectBuilder[Event, State] {

  override def thenRun(callback: State => Unit): EffectBuilder[Event, State] = {
    val sideEffect = new Callback(callback)
    decideEnsureConsistency(
      noNeed = {
        copy(sideEffects = sideEffects :+ sideEffect)
      },
      need = {
        copy(EnsureConsistencyEffect(), sideEffects :+ sideEffect)
      },
    )
  }

  override def thenPassivate(): EffectBuilder[Event, State] =
    copy(sideEffects = sideEffects :+ PassivateEffect())

  override def thenStopLocally(): EffectBuilder[Event, State] =
    copy(sideEffects = sideEffects :+ StopLocallyEffect())

  override def thenUnstashAll(): EffectBuilder[Event, State] =
    copy(sideEffects = sideEffects :+ UnstashAllEffect())

  override def thenReply[Reply](replyTo: ActorRef[Reply])(replyMessage: State => Reply): Effect[Event, State] = {
    val sideEffect = new ReplyEffect[Reply, State](replyTo, replyMessage)
    decideEnsureConsistency(
      noNeed = {
        EffectImpl(mainEffect, sideEffects :+ sideEffect)
      },
      need = {
        EffectImpl(EnsureConsistencyEffect(), sideEffects :+ sideEffect)
      },
    )
  }

  override def thenNoReply(): Effect[Event, State] = effect.EffectImpl(mainEffect, sideEffects)

  /**
    * SideEffects that may affect outside the entity world must ensure consistency
    */
  private[this] def decideEnsureConsistency[T](noNeed: => T, need: => T): T =
    mainEffect match {
      case _: ReplicateEffect[Event, State]         => noNeed
      case _: ReplicateNothingEffect[Event, State]  => need
      case _: EnsureConsistencyEffect[Event, State] => noNeed // e.g. Effect.none.thenRun(...).thenReply(...)(...)
      case _: StashEffect[Event, State] =>
        throw new IllegalStateException("Must not affect outside the entity when stash")
    }
}
