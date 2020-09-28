package lerna.akka.entityreplication.raft.routing

import akka.actor.{ Actor, ActorLogging, ActorRef, Props, RootActorPath }
import akka.cluster.ClusterEvent._
import akka.cluster.{ Cluster, Member }
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing._
import lerna.akka.entityreplication.ReplicationRegion

object ShardingRouter {

  def props(
      extractShardId: ReplicationRegion.ExtractNormalizedShardId,
      memberIndex: MemberIndex,
      replicationRegion: ActorRef,
  ): Props =
    Props(new ShardingRouter(extractShardId, memberIndex, replicationRegion))

  def name(memberIndex: MemberIndex): String = s"ShardingRouter-$memberIndex"
}

class ShardingRouter(
    extractShardId: ReplicationRegion.ExtractNormalizedShardId,
    memberIndex: MemberIndex,
    replicationRegion: ActorRef,
) extends Actor
    with ActorLogging {

  private[this] def hashMapping: ConsistentHashMapping = PartialFunction(extractShardId.andThen(_.underlying))
  private[this] var router: Router =
    Router(ConsistentHashingRoutingLogic(context.system, virtualNodesFactor = 256, hashMapping), Vector())

  private[this] val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp], classOf[MemberRemoved])
  }

  override def receive: Receive = {
    case clusterMemberEvent: MemberEvent =>
      handleClusterMemberEvent(clusterMemberEvent)
    case msg =>
      router.route(msg, sender())
  }

  def handleClusterMemberEvent(clusterMemberEvent: MemberEvent): Unit =
    clusterMemberEvent match {
      case MemberUp(member) if member.roles.contains(memberIndex.role) =>
        router = router.addRoutee(createRegionRoutee(member))
      case MemberUp(_) => // ignore
      case MemberRemoved(member, _) if member.roles.contains(memberIndex.role) =>
        router = router.removeRoutee(createRegionRoutee(member))
      case MemberRemoved(_, _) => // ignore
      case other               => super.unhandled(other)
    }

  def createRegionRoutee(member: Member): Routee = {
    // ActorPath からハッシュ値を計算するため、各ノードで Routee の ActorPath が一致する必要がある。
    // ActorPath が確実に一致するよう、全 Routee で統一的に ActorSelection を使う
    ActorSelectionRoutee(context.actorSelection(RootActorPath(member.address) / replicationRegion.path.elements))
  }
}
