package lerna.akka.entityreplication.typed

import akka.actor.typed.ActorRef

object Effect {

  /**
    * Replicate an event
    */
  def replicate[Event, State](event: Event): EffectBuilder[Event, State] = ???

  /**
    * Do not replicate anything
    */
  def none[Event, State]: EffectBuilder[Event, State] = ???

  /**
    * Do not handle this command, but it is not an error.
    */
  def unhandled[Event, State]: EffectBuilder[Event, State] = ???

  /**
    * Passivate (stop temporarily) this entity to reduce memory consumption
    */
  def passivate[Event, State](): EffectBuilder[Event, State] = ???

  /**
    * Unstash the commands that were stashed with [[Effect.stash()]]
    */
  def unstashAll[Event, State](): EffectBuilder[Event, State] = ???

  /**
    * Stash the this command. Can unstash the command later with [[Effect.unstashAll()]]
    */
  def stash[Event, State](): Effect[Event, State] = ???

  /**
    * Send a reply message to this command
    */
  def reply[Reply, Event, State](replyTo: ActorRef[Reply])(replyMessage: Reply): Effect[Event, State] = ???

  /**
    * Do not send a reply message to this command
    */
  def noReply[Event, State]: Effect[Event, State] = ???
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
