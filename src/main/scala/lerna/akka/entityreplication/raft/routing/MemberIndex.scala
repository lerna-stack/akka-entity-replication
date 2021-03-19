package lerna.akka.entityreplication.raft.routing

import java.net.{ URLDecoder, URLEncoder }

object MemberIndex {
  def apply(role: String): MemberIndex = new MemberIndex(URLEncoder.encode(role, "utf-8"))
}

final case class MemberIndex private (role: String) {
  override def toString: String          = role
  private[entityreplication] def rawRole = URLDecoder.decode(role, "utf-8")
}
