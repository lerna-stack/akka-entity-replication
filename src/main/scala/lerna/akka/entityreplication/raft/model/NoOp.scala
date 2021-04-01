package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.ClusterReplicationSerializable

private[entityreplication] case object NoOp extends ClusterReplicationSerializable
