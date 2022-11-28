package lerna.akka.entityreplication.raft.eventsourced

import lerna.akka.entityreplication.ClusterReplicationSerializable

/**
  * index を揃えるために InternalEvent も永続化必要
  */
private[entityreplication] case object InternalEvent extends ClusterReplicationSerializable
