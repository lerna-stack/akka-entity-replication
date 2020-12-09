package lerna.akka.entityreplication.raft.model

import akka.actor.ActorRef
import lerna.akka.entityreplication.model.EntityInstanceId

case class ClientContext(ref: ActorRef, instanceId: Option[EntityInstanceId], originSender: Option[ActorRef])
