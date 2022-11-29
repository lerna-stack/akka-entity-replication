package lerna.akka.entityreplication.typed

import akka.actor.typed.Signal
import akka.lerna.DeferredBehavior
import lerna.akka.entityreplication.typed.internal.behavior.ReplicatedEntityBehaviorImpl

object ReplicatedEntityBehavior {

  type CommandHandler[Command, Event, State] = (State, Command) => Effect[Event, State]

  type EventHandler[State, Event] = (State, Event) => State

  def apply[Command, Event, State](
      entityContext: ReplicatedEntityContext[Command],
      emptyState: State,
      commandHandler: CommandHandler[Command, Event, State],
      eventHandler: EventHandler[State, Event],
  ): ReplicatedEntityBehavior[Command, Event, State] =
    new ReplicatedEntityBehaviorImpl[Command, Event, State](
      entityContext,
      emptyState,
      commandHandler,
      eventHandler,
    )
}

trait ReplicatedEntityBehavior[Command, Event, State] extends DeferredBehavior[Command] {

  def entityContext: ReplicatedEntityContext[Command]

  def receiveSignal(
      signalHandler: PartialFunction[(State, Signal), Unit],
  ): ReplicatedEntityBehavior[Command, Event, State]

  def signalHandler: PartialFunction[(State, Signal), Unit]

  def withStopMessage(message: Command): ReplicatedEntityBehavior[Command, Event, State]
}
