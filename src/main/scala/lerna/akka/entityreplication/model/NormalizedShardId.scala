package lerna.akka.entityreplication.model

import java.net.URLEncoder

object NormalizedShardId {
  def from(shardId: String): NormalizedShardId = new NormalizedShardId(URLEncoder.encode(shardId, "utf-8"))
}

final case class NormalizedShardId private (underlying: String) extends AnyVal
