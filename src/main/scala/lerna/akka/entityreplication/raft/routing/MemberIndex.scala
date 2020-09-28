package lerna.akka.entityreplication.raft.routing

final case class MemberIndex(role: String) {
  override def toString: String = role
}
