package lerna.akka.entityreplication.raft.eventhandler

/**
  * index を揃えるために InternalEvent も永続化必要
  */
case object InternalEvent
