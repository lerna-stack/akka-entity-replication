package lerna.akka.entityreplication.typed.internal

import akka.actor.ActorRefProvider
import akka.actor.typed.{ ActorRef, ActorSystem, Scheduler }
import akka.lerna.{ InternalActorRefProxy, InternalRecipientRef }
import akka.pattern.StatusReply
import akka.util.{ ByteString, Timeout }
import lerna.akka.entityreplication.typed.{ ReplicatedEntityRef, ReplicatedEntityTypeKey, ReplicationEnvelope }
import akka.actor.typed.scaladsl.AskPattern._
import lerna.akka.entityreplication.util.ActorIds

import java.net.URLEncoder
import scala.concurrent.Future

private[entityreplication] class ReplicatedEntityRefImpl[-M](
    override val typeKey: ReplicatedEntityTypeKey[M],
    override val entityId: String,
    replicationRegion: ActorRef[ReplicationEnvelope[M]],
    system: ActorSystem[_],
) extends ReplicatedEntityRef[M]
    with InternalRecipientRef[M] {

  override def refPrefix: String =
    ActorIds.actorName(Seq(typeKey.name, entityId).map(URLEncoder.encode(_, ByteString.UTF_8)): _*)

  private[this] implicit val scheduler: Scheduler = system.scheduler

  override def tell(message: M): Unit =
    replicationRegion ! ReplicationEnvelope(entityId, message)

  override def ask[Reply](message: ActorRef[Reply] => M)(implicit timeout: Timeout): Future[Reply] = {
    replicationRegion.ask[Reply](replyTo => ReplicationEnvelope(entityId, message(replyTo)))
  }

  override def askWithStatus[Reply](
      massage: ActorRef[StatusReply[Reply]] => M,
  )(implicit timeout: Timeout): Future[Reply] = {
    replicationRegion.askWithStatus[Reply](replyTo => ReplicationEnvelope(entityId, massage(replyTo)))
  }

  private[this] val internalActorRef = InternalActorRefProxy(replicationRegion)

  override def provider: ActorRefProvider = internalActorRef.provider

  override def isTerminated: Boolean = internalActorRef.isTerminated

  override def toString: String = s"ReplicatedEntityRef($typeKey, $entityId)"

  override def equals(other: Any): Boolean =
    other match {
      case that: ReplicatedEntityRefImpl[_] =>
        entityId == that.entityId &&
        typeKey == that.typeKey
      case _ => false
    }

  override def hashCode(): Int = {
    val state: Seq[Any] = Seq(entityId, typeKey)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
