package lerna.akka.entityreplication.typed.internal.effect

private[entityreplication] sealed abstract class MainEffect[+Event, State] {
  def event: Option[Event]
}

private[entityreplication] final case class ReplicateEffect[+Event, State](_event: Event)
    extends MainEffect[Event, State] {

  override def event: Option[Event] = Option(_event)

  override def toString: String = s"Replicate(${_event.getClass.getName})"
}

private[entityreplication] final case class ReplicateNothingEffect[+Event, State]() extends MainEffect[Event, State] {

  override def event: Option[Event] = None

  override def toString: String = "ReplicateNothing"
}

private[entityreplication] final case class EnsureConsistencyEffect[+Event, State]() extends MainEffect[Event, State] {

  override def event: Option[Event] = None

  override def toString: String = "EnsureConsistency"
}

private[entityreplication] final case class StashEffect[+Event, State]() extends MainEffect[Event, State] {

  override def event: Option[Event] = None

  override def toString: String = "Stash"
}
