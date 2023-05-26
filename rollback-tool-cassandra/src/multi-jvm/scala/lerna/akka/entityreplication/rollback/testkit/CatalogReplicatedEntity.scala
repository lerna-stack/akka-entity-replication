package lerna.akka.entityreplication.rollback.testkit

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.event.Logging
import akka.persistence.journal.{ Tagged, WriteEventAdapter }
import lerna.akka.entityreplication.rollback.JsonSerializable
import lerna.akka.entityreplication.typed.{
  Effect,
  ReplicatedEntity,
  ReplicatedEntityBehavior,
  ReplicatedEntityTypeKey,
  ReplicationEnvelope,
}

/** Test implementation of [[ReplicatedEntity]]
  *
  * It holds a set of integers and provides commands to modify the set.
  */
object CatalogReplicatedEntity {
  val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("catalog")

  sealed trait Command                                      extends JsonSerializable
  final case class Add(value: Int, replyTo: ActorRef[Done]) extends Command
  final case class Get(replyTo: ActorRef[GetResponse])      extends Command
  final case class GetResponse(values: Set[Int])            extends JsonSerializable

  sealed trait Event extends JsonSerializable {
    def entityId: String
  }
  final case class Added(entityId: String, value: Int) extends Event

  final case class State(entityId: String, values: Set[Int]) extends JsonSerializable

  def apply(): ReplicatedEntity[Command, ReplicationEnvelope[Command]] =
    ReplicatedEntity(typeKey)(entityContext =>
      Behaviors.setup { context =>
        context.setLoggerName(CatalogReplicatedEntity.getClass)
        ReplicatedEntityBehavior[Command, Event, State](
          entityContext = entityContext,
          emptyState = State(entityContext.entityId, Set.empty),
          commandHandler = commandHandler,
          eventHandler = eventHandler,
        )
      },
    )

  // NOTE: Command Handler should be idempotent.
  private def commandHandler(state: State, command: Command): Effect[Event, State] =
    command match {
      case Add(value, replyTo) =>
        if (state.values.contains(value)) {
          Effect.none.thenReply(replyTo) { _ => Done }
        } else {
          Effect
            .replicate(Added(state.entityId, value))
            .thenReply(replyTo)(_ => Done)
        }
      case Get(replyTo) =>
        Effect.reply(replyTo)(GetResponse(state.values))
    }

  private def eventHandler(state: State, event: Event): State =
    event match {
      case Added(_, value) =>
        state.copy(values = state.values + value)
    }

  object EventAdapter {
    val tag: String = "testkit-catalog"
    val config: String = {
      s"""
         |event-adapters {
         |  testkit-catalog-tagging = "lerna.akka.entityreplication.rollback.testkit.CatalogReplicatedEntity$$EventAdapter"
         |}
         |event-adapter-bindings {
         |  "lerna.akka.entityreplication.rollback.testkit.CatalogReplicatedEntity$$Event" = testkit-catalog-tagging
         |}
         |""".stripMargin
    }
  }

  final class EventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {
    private val log                           = Logging(system, getClass)
    override def manifest(event: Any): String = ""
    override def toJournal(event: Any): Any =
      event match {
        case event: CatalogReplicatedEntity.Event =>
          Tagged(event, tags = Set(EventAdapter.tag))
        case _ =>
          log.warning("Got unexpected event [{}]", event)
          event
      }
  }

}
