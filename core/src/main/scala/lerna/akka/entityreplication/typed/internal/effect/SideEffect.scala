package lerna.akka.entityreplication.typed.internal.effect

import akka.actor.typed.ActorRef

private[entityreplication] sealed abstract class SideEffect[State]

private[entityreplication] class Callback[State](val sideEffect: State => Unit) extends SideEffect[State] {
  override def toString: String = "Callback"
}

private[entityreplication] final class ReplyEffect[Reply, State](
    replyTo: ActorRef[Reply],
    message: State => Reply,
) extends Callback[State](state => replyTo ! message(state)) {
  override def toString: String = s"Reply(${message.getClass.getName})"
}

private[entityreplication] final class NoReplyEffect[State] extends SideEffect[State] {
  override def toString: String = "NoReply"
}

/**
  * [[UnhandledEffect]] will send the command to the event stream in Akka
  */
private[entityreplication] final class UnhandledEffect[State] extends SideEffect[State] {
  override def toString: String = "Unhandled"
}

private[entityreplication] case class PassivateEffect[State]() extends SideEffect[State] {
  override def toString: String = "Passivate"
}

private[entityreplication] case class StopLocallyEffect[State]() extends SideEffect[State] {
  override def toString: String = "StopLocally"
}

private[entityreplication] case class UnstashAllEffect[State]() extends SideEffect[State] {
  override def toString: String = "UnstashAll"
}
