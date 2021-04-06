package lerna.akka.entityreplication.raft.routing

import java.net.URLEncoder

private[entityreplication] object MemberIndex {
  def apply(role: String): MemberIndex                                              = new MemberIndex(URLEncoder.encode(role, "utf-8"))
  private[entityreplication] def fromEncodedValue(encodedRole: String): MemberIndex = new MemberIndex(encodedRole)
}

private[entityreplication] final case class MemberIndex private (role: String) {
  override def toString: String = role
}
