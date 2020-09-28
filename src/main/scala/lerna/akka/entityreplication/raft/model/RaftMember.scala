package lerna.akka.entityreplication.raft.model

import akka.actor.{ ActorPath, ActorSelection }

case class RaftMember(path: ActorPath, selection: ActorSelection) {

  override def toString: String = s"RaftMember(${path.toString})"
}
