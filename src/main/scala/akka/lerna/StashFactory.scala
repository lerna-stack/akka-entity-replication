package akka.lerna

import akka.actor.{ ActorContext, ActorRef, StashSupport }

/** Proxy for using `def createStash` from [[lerna]] package
  */
trait StashFactory extends akka.actor.StashFactory { this: akka.actor.Actor =>
  override final protected[akka] def createStash()(implicit ctx: ActorContext, ref: ActorRef): StashSupport =
    super.createStash()(ctx, ref)
}
