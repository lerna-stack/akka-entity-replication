package lerna.akka.entityreplication.typed

import akka.actor.typed.ActorRef
import lerna.akka.entityreplication.typed.internal.effect._

object Effect {

  /**
    * Replicate an event
    */
  def replicate[Event, State](event: Event): EffectBuilder[Event, State] =
    EffectBuilderImpl(ReplicateEffect(event))

  /**
    * Do not replicate anything
    */
  def none[Event, State]: EffectBuilder[Event, State] =
    EffectBuilderImpl(ReplicateNothingEffect())

  /**
    * Do not handle this command, but it is not an error.
    */
  def unhandled[Event, State]: EffectBuilder[Event, State] =
    EffectBuilderImpl.unhandled(ReplicateNothingEffect())

  /**
    * Passivate (stop temporarily) this entity to reduce memory consumption
    */
  def passivate[Event, State](): EffectBuilder[Event, State] =
    none.thenPassivate()

  /**
    * Unstash the commands that were stashed with [[Effect.stash()]]
    */
  def unstashAll[Event, State](): EffectBuilder[Event, State] =
    none.thenUnstashAll()

  /**
    * Stash the this command. Can unstash the command later with [[Effect.unstashAll()]]
    */
  def stash[Event, State](): Effect[Event, State] =
    EffectBuilderImpl(StashEffect[Event, State]()).thenNoReply()

  /**
    * Send a reply message to this command
    */
  def reply[Reply, Event, State](replyTo: ActorRef[Reply])(replyMessage: Reply): Effect[Event, State] =
    none.thenReply(replyTo)(_ => replyMessage)

  /**
    * Do not send a reply message to this command
    */
  def noReply[Event, State]: Effect[Event, State] =
    none.thenNoReply()
}

trait Effect[+Event, State] {

  /**
    * An event that will be replicated.
    */
  def event: Option[Event]
}

trait EffectBuilder[+Event, State] {

  /**
    * Run the callback after complete event replication
    */
  def thenRun(callback: State => Unit): EffectBuilder[Event, State]

  /**
    * Passivate (stop temporarily) this entity to reduce memory consumption
    */
  def thenPassivate(): EffectBuilder[Event, State]

  /**
    * Unstash the commands that were stashed with [[Effect.stash()]]
    */
  def thenUnstashAll(): EffectBuilder[Event, State]

  /**
    * Send a reply message to this command. Can create the message from the state that was applied the replicated event
    */
  def thenReply[Reply](replyTo: ActorRef[Reply])(replyMessage: State => Reply): Effect[Event, State]

  /**
    * Do not send a reply message to this command
    */
  def thenNoReply(): Effect[Event, State]
}
