package akka.lerna

/**
  * Expose internal API of Akka to use it in [[lerna]] package.
  */
trait InternalRecipientRef[-T] extends akka.actor.typed.internal.InternalRecipientRef[T]
