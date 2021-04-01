package lerna.akka.entityreplication.model

import akka.actor.ActorPath
import lerna.akka.entityreplication.ReplicationRegion.EntityId

import java.net.URLEncoder

private[entityreplication] object NormalizedEntityId {
  def from(entityId: EntityId): NormalizedEntityId = new NormalizedEntityId(URLEncoder.encode(entityId, "utf-8"))

  def of(entityPath: ActorPath): NormalizedEntityId = new NormalizedEntityId(entityPath.name)

  private[entityreplication] def fromEncodedValue(encodedEntityId: EntityId): NormalizedEntityId =
    new NormalizedEntityId(encodedEntityId)
}

private[entityreplication] final case class NormalizedEntityId private (underlying: String) extends AnyVal
