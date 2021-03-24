package lerna.akka.entityreplication.model

import java.net.{ URLDecoder, URLEncoder }

import akka.actor.ActorPath

object NormalizedShardId {
  def from(shardId: String): NormalizedShardId = new NormalizedShardId(URLEncoder.encode(shardId, "utf-8"))

  private[entityreplication] def from(path: ActorPath) = new NormalizedShardId(path.name)

  private[entityreplication] def fromEncodedValue(encodedShardId: String): NormalizedShardId =
    new NormalizedShardId(encodedShardId)
}

final case class NormalizedShardId private (underlying: String) extends AnyVal {
  def raw: String = URLDecoder.decode(underlying, "utf-8")
}
