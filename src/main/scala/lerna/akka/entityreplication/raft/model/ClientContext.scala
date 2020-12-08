package lerna.akka.entityreplication.raft.model

import akka.actor.ActorRef

case class ClientContext(ref: ActorRef, originSender: Option[ActorRef])
