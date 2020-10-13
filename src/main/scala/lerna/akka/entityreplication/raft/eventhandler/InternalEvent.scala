package lerna.akka.entityreplication.raft.eventhandler

import lerna.akka.entityreplication.ClusterReplicationSerializable

/**
  * index を揃えるために InternalEvent も永続化必要
  */
case object InternalEvent extends ClusterReplicationSerializable
