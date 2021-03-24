package lerna.akka.entityreplication.raft.routing

import java.net.URLEncoder

object MemberIndex {
  def apply(role: String): MemberIndex                                              = new MemberIndex(URLEncoder.encode(role, "utf-8"))
  private[entityreplication] def fromEncodedValue(encodedRole: String): MemberIndex = new MemberIndex(encodedRole)
}

final case class MemberIndex private (role: String) {
  override def toString: String = role
}
