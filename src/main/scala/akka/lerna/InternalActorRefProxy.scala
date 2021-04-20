package akka.lerna

import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._

object InternalActorRefProxy {
  def apply[T](ref: ActorRef[T]): InternalActorRefProxy[T] =
    new InternalActorRefProxy[T](ref)
}

class InternalActorRefProxy[T](ref: ActorRef[T]) {

  private[this] val classicRef = ref.toClassic.asInstanceOf[akka.actor.InternalActorRef]

  def provider: ActorRefProvider = classicRef.provider

  def isTerminated: Boolean = classicRef.isTerminated
}
