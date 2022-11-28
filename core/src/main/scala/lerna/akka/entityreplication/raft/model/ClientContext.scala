package lerna.akka.entityreplication.raft.model

import akka.actor.ActorRef
import lerna.akka.entityreplication.model.EntityInstanceId

private[entityreplication] final case class ClientContext(
    ref: ActorRef,
    instanceId: Option[EntityInstanceId],
    originSender: Option[ActorRef],
) {

  /** Sends the given `message` to the actor `ref`, including the sender `originSender`
    *
    * If `originSender` is `None`, [[ActorRef.noSender]] is included as the sender.
    */
  def forward(message: Any): Unit = {
    ref.tell(message, originSender.getOrElse(ActorRef.noSender))
  }

}
