package lerna.akka.entityreplication.model

import java.net.{ URLDecoder, URLEncoder }
import akka.actor.ActorPath
import lerna.akka.entityreplication.ReplicationRegion.EntityId

object NormalizedEntityId {
  def from(entityId: EntityId): NormalizedEntityId = new NormalizedEntityId(URLEncoder.encode(entityId, "utf-8"))

  def of(entityPath: ActorPath): NormalizedEntityId = new NormalizedEntityId(entityPath.name)
}

final case class NormalizedEntityId private (underlying: String) extends AnyVal {
  private[entityreplication] def raw: String = URLDecoder.decode(underlying, "utf-8")
}
