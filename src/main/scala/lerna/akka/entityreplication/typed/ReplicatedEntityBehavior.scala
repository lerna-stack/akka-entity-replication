package lerna.akka.entityreplication.typed

import akka.actor.typed.{ Behavior, Signal }

object ReplicatedEntityBehavior {

  type CommandHandler[Command, Event, State] = (State, Command) => Effect[Event, State]

  type EventHandler[State, Event] = (State, Event) => State

  def apply[Command, Event, State](
      replicationId: ReplicationId,
      emptyState: State,
      commandHandler: CommandHandler[Command, Event, State],
      eventHandler: EventHandler[State, Event],
  ): ReplicatedEntityBehavior[Command, Event, State] = ???
}

trait ReplicatedEntityBehavior[Command, Event, State] extends Behavior[Command] {

  def replicationId: ReplicationId

  def receiveSignal(
      signalHandler: PartialFunction[(State, Signal), Unit],
  ): ReplicatedEntityBehavior[Command, Event, State]

  def signalHandler: PartialFunction[(State, Signal), Unit]
}
