package akka.lerna

import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.internal.adapter.ActorRefAdapter

object InternalActorRefProxy {
  def apply[T](ref: ActorRef[T]): InternalActorRefProxy[T] =
    new InternalActorRefProxy[T](ref)
}

class InternalActorRefProxy[T](ref: ActorRef[T]) {

  private[this] val classicRef = ActorRefAdapter.toClassic(ref)

  def provider: ActorRefProvider = classicRef.provider

  def isTerminated: Boolean = classicRef.isTerminated
}
