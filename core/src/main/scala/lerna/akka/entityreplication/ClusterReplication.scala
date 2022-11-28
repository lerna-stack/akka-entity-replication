package lerna.akka.entityreplication

import akka.actor.{ Actor, ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props, Status }
import lerna.akka.entityreplication.ClusterReplication.EntityPropsProvider
import lerna.akka.entityreplication.model.TypeName
import lerna.akka.entityreplication.raft.eventsourced.CommitLogStoreActor
import lerna.akka.entityreplication.util.ActorIds
import akka.util.Timeout
import akka.pattern.ask
import lerna.akka.entityreplication.ClusterReplicationGuardian.Started

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object ClusterReplication extends ExtensionId[ClusterReplication] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = ClusterReplication

  override def createExtension(system: ExtendedActorSystem): ClusterReplication = new ClusterReplication(system)

  private[entityreplication] type EntityPropsProvider = ReplicationActorContext => Props
}

class ClusterReplication private (system: ExtendedActorSystem) extends Extension {

  import ClusterReplication._

  private[this] lazy val guardian: ActorRef =
              system.systemActorOf(ClusterReplicationGuardian.props(), "clusterReplicationGuardian")

  @deprecated(message = "Use typed.ClusterReplication.init() instead", since = "2.0.0")
  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterReplicationSettings,
      extractEntityId: ReplicationRegion.ExtractEntityId,
      extractShardId: ReplicationRegion.ExtractShardId,
  ): ActorRef = {
    val possibleShardIds = Set.empty[ReplicationRegion.ShardId]
    internalStart(typeName, _ => entityProps, settings, extractEntityId, extractShardId, possibleShardIds)
  }

  private[entityreplication] def internalStart(
      typeName: String,
      entityProps: EntityPropsProvider,
      settings: ClusterReplicationSettings,
      extractEntityId: ReplicationRegion.ExtractEntityId,
      extractShardId: ReplicationRegion.ExtractShardId,
      possibleShardIds: Set[ReplicationRegion.ShardId],
  ): ActorRef = {

    implicit val timeout = Timeout(30.seconds)

    val start = ClusterReplicationGuardian.Start(
      typeName = typeName,
      entityProps,
      settings,
      extractEntityId,
      extractShardId,
      possibleShardIds,
    )

    Await.result((guardian ? start).mapTo[Started], timeout.duration).regionRef
  }
}

private[entityreplication] object ClusterReplicationGuardian {
  def props(): Props = Props(new ClusterReplicationGuardian())

  sealed trait Command

  final case class Start(
      typeName: String,
      entityProps: EntityPropsProvider,
      settings: ClusterReplicationSettings,
      extractEntityId: ReplicationRegion.ExtractEntityId,
      extractShardId: ReplicationRegion.ExtractShardId,
      possibleShardIds: Set[ReplicationRegion.ShardId],
  ) extends Command

  final case class Started(regionRef: ActorRef)
}

private[entityreplication] class ClusterReplicationGuardian extends Actor {
  import ClusterReplicationGuardian._

  override def receive: Receive = {

    case Start(typeName, entityProps, settings, extractEntityId, extractShardId, possibleShardIds) =>
      try {
        val _typeName  = TypeName.from(typeName)
        val regionName = ActorIds.actorName(_typeName.underlying)

        val commitLogStore: ActorRef =
          CommitLogStoreActor.startClusterSharding(_typeName, context.system, settings)

        val regionRef: ActorRef =
          context.child(regionName) match {
            case Some(ref) => ref
            case None =>
              context.actorOf(
                ReplicationRegion.props(
                  typeName,
                  entityProps,
                  settings,
                  extractEntityId,
                  extractShardId,
                  possibleShardIds,
                  commitLogStore,
                ),
                regionName,
              )
          }

        sender() ! Started(regionRef)
      } catch {
        case NonFatal(ex) =>
          sender() ! Status.Failure(ex)
      }
  }
}
