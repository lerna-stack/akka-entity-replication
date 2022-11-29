package lerna.akka.entityreplication.typed.internal.effect

import lerna.akka.entityreplication.typed.Effect

import scala.collection.immutable

private[entityreplication] case class EffectImpl[+Event, State](
    mainEffect: MainEffect[Event, State],
    sideEffects: immutable.Seq[SideEffect[State]],
) extends Effect[Event, State] {
  override def event: Option[Event] = mainEffect.event
}
