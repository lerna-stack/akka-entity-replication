package akka.lerna

/**
  * Expose internal API of Akka to use it in [[lerna]] package.
  */
abstract class DeferredBehavior[Command] extends akka.actor.typed.internal.BehaviorImpl.DeferredBehavior[Command]
