package lerna.akka.entityreplication.typed.internal.testkit

import lerna.akka.entityreplication.typed.testkit.ReplicatedEntityBehaviorTestKit

import scala.reflect.ClassTag

private[entityreplication] final case class CommandResultImpl[Command, Event, State, Reply](
    command: Command,
    eventOption: Option[Event],
    state: State,
    replyOption: Option[Reply],
) extends ReplicatedEntityBehaviorTestKit.CommandResultWithReply[Command, Event, State, Reply] {

  override def hasNoEvents: Boolean = eventOption.isEmpty

  override def event: Event = eventOption.getOrElse(throw new AssertionError("No event"))

  override def eventOfType[E <: Event: ClassTag]: E = ofType(event, "event")

  override def stateOfType[S <: State: ClassTag]: S = ofType(state, "state")

  override def reply: Reply = replyOption.getOrElse(throw new AssertionError("No reply"))

  override def replyOfType[R <: Reply: ClassTag]: R = ofType(reply, "reply")

  private[this] def ofType[T: ClassTag](obj: Any, objCategory: String): T = {
    obj match {
      case t: T => t
      case other =>
        val expectedClass = implicitly[ClassTag[T]].runtimeClass
        throw new AssertionError(
          s"Expected $objCategory class [${expectedClass.getName}], but was [${other.getClass.getName}]",
        )
    }
  }
}
