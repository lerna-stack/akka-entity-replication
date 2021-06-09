package lerna.akka.entityreplication.typed

import akka.actor.typed.{ ActorRef, RecipientRef }
import akka.lerna.InternalRecipientRef
import akka.pattern.StatusReply
import akka.util.Timeout

import scala.concurrent.Future

trait ReplicatedEntityRef[-M] extends RecipientRef[M] { this: InternalRecipientRef[M] =>

  /**
    * The identifier for the entity referenced by this [[ReplicatedEntityRef]]
    */
  def entityId: String

  /**
    * The [[ReplicatedEntityTypeKey]] associated with this [[ReplicatedEntityRef]]
    */
  def typeKey: ReplicatedEntityTypeKey[M]

  /**
    * Send a message to the entity referenced by this [[ReplicatedEntityRef]] using *at-most-once* messaging semantics.
    */
  def tell(message: M): Unit

  /**
    * Send a message to the entity referenced by this [[ReplicatedEntityRef]] using *at-most-once* messaging semantics.
    */
  def !(message: M): Unit = this.tell(message)

  /**
    * Allows to "ask" this [[ReplicatedEntityRef]] for a reply.
    *
    * Note that if you are implementing an actor, you should prefer [[akka.actor.typed.scaladsl.ActorContext.ask]]
    * as that provides better safety.
    *
    * Example usage:
    * {{{
    * case class Echo(message: String, replyTo: ActorRef[EchoResponse])
    * case class EchoResponse(message: String)
    *
    * implicit val timeout = Timeout(3.seconds)
    * val target: ReplicatedEntityRef[Echo] = ...
    * val response: Future[EchoResponse] = target.ask(Echo("Hello, world", _))
    * }}}
    */
  def ask[Reply](message: ActorRef[Reply] => M)(implicit timeout: Timeout): Future[Reply]

  /**
    * The same as [[ask]] but only for requests that result in a response of type [[akka.pattern.StatusReply]].
    * If the response is a [[akka.pattern.StatusReply.Success]] the returned future is completed successfully with the wrapped response.
    * If the response is a [[akka.pattern.StatusReply.Error]] the returned future will be failed with the exception
    * in the error (normally a [[akka.pattern.StatusReply.ErrorMessage]])
    */
  def askWithStatus[Reply](massage: ActorRef[StatusReply[Reply]] => M)(implicit timeout: Timeout): Future[Reply]

  /**
    * Allows to "ask" this [[ReplicatedEntityRef]] for a reply.
    *
    * Note that if you are implementing an actor, you should prefer [[akka.actor.typed.scaladsl.ActorContext.ask]]
    * as that provides better safety.
    *
    * Example usage:
    * {{{
    * case class Echo(message: String, replyTo: ActorRef[EchoResponse])
    * case class EchoResponse(message: String)
    *
    * implicit val timeout = Timeout(3.seconds)
    * val target: ReplicatedEntityRef[Echo] = ...
    * val response: Future[EchoResponse] = target ? (replyTo => Echo("Hello, world", replyTo))
    * }}}
    */
  def ?[Reply](message: ActorRef[Reply] => M)(implicit timeout: Timeout): Future[Reply] =
    this.ask(message)(timeout)
}
