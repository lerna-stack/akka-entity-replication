package lerna.akka.entityreplication.typed.internal

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior, BehaviorInterceptor, TypedActorContext }
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.raft.RaftProtocol
import lerna.akka.entityreplication.raft.RaftProtocol.ProcessCommand
import lerna.akka.entityreplication.typed.ClusterReplication
import lerna.akka.{ entityreplication => untyped }
import lerna.akka.entityreplication.typed._

import scala.jdk.CollectionConverters._
import scala.collection.concurrent
import java.util.concurrent.ConcurrentHashMap

private[entityreplication] class ClusterReplicationImpl(system: ActorSystem[_]) extends ClusterReplication {

  private[this] val regions
      : concurrent.Map[ReplicatedEntityTypeKey[Nothing], (ClusterReplicationSettings, ActorRef[Nothing])] =
    new ConcurrentHashMap[ReplicatedEntityTypeKey[_], (ClusterReplicationSettings, ActorRef[_])].asScala

  override def init[M, E](entity: ReplicatedEntity[M, E]): ActorRef[E] =
    regions
      .getOrElseUpdate(
        entity.typeKey,
        (
          entity.settings.getOrElse(ClusterReplicationSettings(system)),
          internalInit(entity),
        ),
      )._2.unsafeUpcast[E]

  private[this] def internalInit[M, E](entity: ReplicatedEntity[M, E]): ActorRef[E] = {
    val classicSystem = system.toClassic
    val settings      = entity.settings.getOrElse(untyped.ClusterReplicationSettings.create(classicSystem))
    val extractEntityId: untyped.ReplicationRegion.ExtractEntityId = {
      case ReplicationEnvelope(entityId, message) => (entityId, message)
    }
    val extractShardId: untyped.ReplicationRegion.ExtractShardId = {
      case ReplicationEnvelope(entityId, _) =>
        shardIdOf(settings, entityId)
    }
    val possibleShardIds: Set[untyped.ReplicationRegion.ShardId] = {
      (0 until settings.raftSettings.numberOfShards).map(_.toString).toSet
    }
    val interceptor = new BehaviorInterceptor[RaftProtocol.EntityCommand, Any] {
      override def aroundReceive(
          ctx: TypedActorContext[RaftProtocol.EntityCommand],
          msg: RaftProtocol.EntityCommand,
          target: BehaviorInterceptor.ReceiveTarget[Any],
      ): Behavior[Any] =
        msg match {
          // Send the command extracted from the ProcessCommand that is sent by RaftActor to ReplicatedEntityBehavior
          // see also: ReplicatedEntityBehaviorImpl
          case ProcessCommand(command) => target(ctx, command)
          case other                   => target(ctx, other)
        }
    }
    val region =
      untyped
        .ClusterReplication(classicSystem).internalStart(
          typeName = entity.typeKey.name,
          entityProps = { context =>
            val entityContext = new ReplicatedEntityContext[M](
              entityTypeKey = entity.typeKey,
              entityId = context.entityId,
              shard = context.shard.toTyped[ClusterReplication.ShardCommand],
            )
            // This behavior has to receive EntityCommands and user defined commands
            // see also: ReplicatedEntityBehaviorImpl
            val behavior = entity.createBehavior(entityContext).asInstanceOf[Behavior[Any]]
            PropsAdapter(Behaviors.intercept(() => interceptor)(behavior))
          },
          settings = settings,
          extractEntityId = extractEntityId,
          extractShardId = extractShardId,
          possibleShardIds = possibleShardIds,
        )
    region.toTyped
  }

  override def entityRefFor[M](typeKey: ReplicatedEntityTypeKey[M], entityId: String): ReplicatedEntityRef[M] =
    regions.get(typeKey) match {
      case Some((_, region)) =>
        new ReplicatedEntityRefImpl[M](typeKey, entityId, region.unsafeUpcast[ReplicationEnvelope[M]], system)
      case None => throw new IllegalStateException(s"The type [${typeKey}] must be init first")
    }

  override def shardIdOf[M](
      typeKey: ReplicatedEntityTypeKey[M],
      entityId: String,
  ): untyped.ReplicationRegion.ShardId = {
    regions.get(typeKey) match {
      case Some((settings, _)) =>
        shardIdOf(settings, entityId)
      case None =>
        throw new IllegalStateException(s"The type [${typeKey}] must be init first")
    }
  }

  private[entityreplication] def shardIdOf(
      settings: untyped.ClusterReplicationSettings,
      entityId: String,
  ): untyped.ReplicationRegion.ShardId =
    Math.abs(entityId.hashCode % settings.raftSettings.numberOfShards).toString
}
