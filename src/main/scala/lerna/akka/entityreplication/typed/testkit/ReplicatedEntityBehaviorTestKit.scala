package lerna.akka.entityreplication.typed.testkit

import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import lerna.akka.entityreplication.typed.{ ReplicatedEntityContext, ReplicatedEntityTypeKey }

import scala.reflect.ClassTag

object ReplicatedEntityBehaviorTestKit {

  /**
    * Creates a [[ReplicatedEntityBehaviorTestKit]]
    */
  def apply[Command, Event, State](
      system: ActorSystem[_],
      entityTypeKey: ReplicatedEntityTypeKey[Command],
      entityId: String,
      behavior: ReplicatedEntityContext[Command] => Behavior[Command],
  ): ReplicatedEntityBehaviorTestKit[Command, Event, State] = ???

  trait CommandResult[Command, Event, State] {

    /**
      * The command that was processed.
      */
    def command: Command

    /**
      * `true` if no events were replicated by the command.
      */
    def hasNoEvents: Boolean

    /**
      * The replicated event.
      */
    def event: Event

    /**
      * The replicated event as a given expected type.
      * It will throw [[AssertionError]] if there is no event or if the event is of a different type.
      */
    def eventOfType[E <: Event: ClassTag]: E

    /**
      * The state after applying the event.
      */
    def state: State

    /**
      * The state after applying the event as a given expected type.
      * It will throw [[AssertionError]] if the state is of a different type.
      */
    def stateOfType[S <: State: ClassTag]: S
  }

  trait CommandResultWithReply[Command, Event, State, Reply] extends CommandResult[Command, Event, State] {

    /**
      * The reply by the command. It will throw [[AssertionError]] if there was no reply.
      */
    def reply: Reply

    /**
      * The reply by the command as a given expected type.
      * It will throw [[AssertionError]] if there is no reply or if the reply is of a different type.
      */
    def replyOfType[R <: Reply: ClassTag]: R
  }

  trait RestartResult[State] {

    /**
      * The state of the behavior after recovery.
      */
    def state: State
  }

}

trait ReplicatedEntityBehaviorTestKit[Command, Event, State] {

  import ReplicatedEntityBehaviorTestKit._

  /**
    * Run one command through the behavior.
    */
  def runCommand(command: Command): CommandResult[Command, Event, State]

  /**
    * Run one command with a `replyTo: ActorRef[R]` through the behavior.
    */
  def runCommand[R](creator: ActorRef[R] => Command): CommandResultWithReply[Command, Event, State, R]

  /**
    * The current state of the behavior.
    */
  def state: State

  /**
    * Restart the behavior, which will then recover from stored snapshot and events.
    */
  def restart(): RestartResult[State]

  /**
    * Clears the journal and snapshot and restart the behavior
    */
  def clear(): Unit
}
