package akka.lerna

import akka.actor.typed.internal.StashBufferImpl
import akka.actor.typed.scaladsl.ActorContext

/**
  * Expose internal API of Akka to use it in [[lerna]] package.
  */
object StashBuffer {

  def apply[T](ctx: ActorContext[T], capacity: Int): akka.actor.typed.scaladsl.StashBuffer[T] =
    StashBufferImpl[T](ctx, capacity)
}
