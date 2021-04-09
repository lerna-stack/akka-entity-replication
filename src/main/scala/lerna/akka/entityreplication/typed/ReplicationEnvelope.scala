package lerna.akka.entityreplication.typed

import akka.actor.WrappedMessage

/**
  * Envelope type that is used by with Cluster Replication
  */
final case class ReplicationEnvelope[M](entityId: String, message: M) extends WrappedMessage
