package lerna.akka.entityreplication.raft.eventhandler

sealed trait CommittedEvent extends Serializable

final case class DomainEvent(event: Any) extends CommittedEvent

/**
  * index を揃えるために InternalEvent も永続化必要
  */
object InternalEvent extends CommittedEvent
