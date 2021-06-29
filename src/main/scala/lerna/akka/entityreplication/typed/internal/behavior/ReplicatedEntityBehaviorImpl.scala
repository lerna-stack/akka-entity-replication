package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.typed.{ ActorRef, Behavior, BehaviorInterceptor, Signal, SupervisorStrategy, TypedActorContext }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import lerna.akka.entityreplication.ClusterReplicationSettings
import lerna.akka.entityreplication.raft.RaftProtocol.{ EntityCommand, ProcessCommand }
import lerna.akka.entityreplication.typed.ClusterReplication.ShardCommand
import lerna.akka.entityreplication.typed.internal.ReplicationId
import lerna.akka.entityreplication.typed.{ ReplicatedEntityBehavior, ReplicatedEntityContext }
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.model.EntityInstanceId
import lerna.akka.entityreplication.typed.internal.behavior.ReplicatedEntityBehaviorImpl.generateInstanceId

import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

private[entityreplication] object ReplicatedEntityBehaviorImpl {

  private[this] val instanceIdCounter = new AtomicInteger(1)

  private def generateInstanceId(): EntityInstanceId = EntityInstanceId(instanceIdCounter.getAndIncrement())
}

private[entityreplication] final case class ReplicatedEntityBehaviorImpl[Command, Event, State](
    entityContext: ReplicatedEntityContext[Command],
    emptyState: State,
    commandHandler: ReplicatedEntityBehavior.CommandHandler[Command, Event, State],
    eventHandler: ReplicatedEntityBehavior.EventHandler[State, Event],
    stopMessage: Option[Command] = None,
    signalHandler: PartialFunction[(State, Signal), Unit] = PartialFunction.empty,
) extends ReplicatedEntityBehavior[Command, Event, State] {

  override def apply(ctx: TypedActorContext[Command]): Behavior[Command] = {
    try {
      val settings = ClusterReplicationSettings(ctx.asScala.system.toClassic)
      Behaviors.intercept(() => interceptor)(createBehavior(entityContext.shard, settings)).narrow
    } catch {
      case NonFatal(e) =>
        ctx.asScala.log.error("ReplicatedEntityBehavior initialization failed", e)
        Behaviors.stopped
    }
  }

  /**
    * needs to accept [[Any]] since [[ReplicatedEntityBehavior]] can receive message from the [[RaftActor]]
    * that is not part of the user defined Command protocol
    */
  def interceptor: BehaviorInterceptor[Any, EntityCommand] =
    new BehaviorInterceptor[Any, EntityCommand]() {
      override def aroundReceive(
          ctx: TypedActorContext[Any],
          msg: Any,
          target: BehaviorInterceptor.ReceiveTarget[EntityCommand],
      ): Behavior[EntityCommand] = {
        msg match {
          // This behavior should not accept ProcessCommand (part of EntityCommand)
          // because some Behavior factories expect that this behavior receive user-defined Command directly.
          //    e.g. Behaviors.withMdc(staticMdc: Map[String, String], mdcForMessage: Command => Map[String, String])
          case command: ProcessCommand =>
            ctx.asScala.log.warn(
              "Unhandled the command: The command [{}] must be extracted from [{}]",
              command.command.getClass.getName,
              command.getClass.getName,
            )
            Behaviors.unhandled
          case command: EntityCommand => target(ctx, command)
          case command                => target(ctx, ProcessCommand(command))
        }
      }

      override def toString: String = "ReplicatedEntityBehaviorInterceptor"
    }

  def createBehavior(shard: ActorRef[ShardCommand], settings: ClusterReplicationSettings): Behavior[EntityCommand] = {
    Behaviors
      .supervise {
        Behaviors.setup { context: ActorContext[EntityCommand] =>
          Behaviors.withStash[EntityCommand](Int.MaxValue /* TODO: Should I get from config? */ ) { buffer =>
            val instanceId = generateInstanceId()
            val setup = new BehaviorSetup(
              entityContext,
              emptyState,
              commandHandler,
              eventHandler,
              signalHandler,
              stopMessage,
              ReplicationId(entityContext.entityTypeKey, entityContext.entityId),
              shard,
              settings,
              context,
              instanceId,
              buffer,
            )
            Recovering.behavior(setup)
          }
        }
      }.onFailure(SupervisorStrategy.restart)
  }

  override def receiveSignal(
      signalHandler: PartialFunction[(State, Signal), Unit],
  ): ReplicatedEntityBehavior[Command, Event, State] = {
    copy(signalHandler = signalHandler)
  }

  override def withStopMessage(message: Command): ReplicatedEntityBehavior[Command, Event, State] =
    copy(stopMessage = Option(message))
}
