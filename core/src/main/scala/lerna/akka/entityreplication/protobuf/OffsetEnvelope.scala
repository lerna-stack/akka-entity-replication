package lerna.akka.entityreplication.protobuf

import akka.persistence.query.{ Sequence, TimeBasedUUID }
import lerna.akka.entityreplication.ClusterReplicationSerializable

private[protobuf] sealed trait OffsetEnvelope
private[protobuf] object NoOffsetEnvelope extends OffsetEnvelope with ClusterReplicationSerializable
private[protobuf] case class SequenceEnvelope(underlying: Sequence)
    extends OffsetEnvelope
    with ClusterReplicationSerializable
private[protobuf] case class TimeBasedUUIDEnvelope(underlying: TimeBasedUUID)
    extends OffsetEnvelope
    with ClusterReplicationSerializable
