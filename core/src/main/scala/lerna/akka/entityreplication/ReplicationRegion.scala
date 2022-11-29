package lerna.akka.entityreplication

import akka.actor.typed.scaladsl.adapter.ClassicActorContextOps
import akka.actor.{ Actor, ActorLogging, ActorPath, ActorRef, OneForOneStrategy, Props, Stash, SupervisorStrategy }
import akka.cluster.ClusterEvent._
import akka.cluster.sharding.ShardRegion.GracefulShutdown
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.cluster.{ Cluster, Member, MemberStatus }
import akka.routing.{ ActorRefRoutee, ConsistentHashingRouter, ConsistentHashingRoutingLogic, Router }
import lerna.akka.entityreplication.ClusterReplication.EntityPropsProvider
import lerna.akka.entityreplication.ReplicationRegion.{ ExtractEntityId, ExtractShardId }
import lerna.akka.entityreplication.model._
import lerna.akka.entityreplication.raft.RaftActor
import lerna.akka.entityreplication.raft.RaftProtocol.{ Command, ForwardedCommand }
import lerna.akka.entityreplication.raft.protocol.ShardRequest
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.raft.snapshot.ShardSnapshotStore
import lerna.akka.entityreplication.typed.ClusterReplication.ShardCommand

import scala.collection.mutable

object ReplicationRegion {

  /**
    * Marker type of entity identifier (`String`).
    */
  type EntityId = String

  /**
    * Marker type of shard identifier (`String`).
    */
  type ShardId = String

  /**
    * Marker type of application messages (`Any`).
    */
  type Msg = Any

  /**
    * Interface of the partial function used by the [[ReplicationRegion]] to
    * extract the entity id and the message to send to the entity from an
    * incoming message. The implementation is application specific.
    */
  type ExtractEntityId = PartialFunction[Msg, (EntityId, Msg)]

  /**
    * Interface of the function used by the [[ReplicationRegion]] to
    * extract the shard id from an incoming message.
    * Only messages that passed the [[ExtractEntityId]] will be used
    * as input to this function.
    */
  type ExtractShardId = PartialFunction[Msg, ShardId]

  private[entityreplication] type ExtractNormalizedShardId = PartialFunction[Msg, NormalizedShardId]

  private[entityreplication] def props(
      typeName: String,
      entityProps: EntityPropsProvider,
      settings: ClusterReplicationSettings,
      extractEntityId: ExtractEntityId,
      extractShardId: ExtractShardId,
      possibleShardIds: Set[ReplicationRegion.ShardId],
      commitLogStore: ActorRef,
  ) =
    Props(
      new ReplicationRegion(
        typeName,
        entityProps,
        settings,
        extractEntityId,
        extractShardId,
        possibleShardIds,
        commitLogStore,
      ),
    )

  private[entityreplication] case class CreateShard(shardId: NormalizedShardId) extends ShardRequest

  final case class Passivate(entityPath: ActorPath, stopMessage: Any) extends ShardCommand

  private[entityreplication] sealed trait RoutingCommand
  private[entityreplication] final case class Broadcast(message: Any)                     extends RoutingCommand
  private[entityreplication] final case class BroadcastWithoutSelf(message: Any)          extends RoutingCommand
  private[entityreplication] final case class DeliverTo(index: MemberIndex, message: Any) extends RoutingCommand
  private[entityreplication] final case class DeliverSomewhere(message: Any)              extends RoutingCommand

  /**
    * [[ReplicationRegion]] 同士の通信で利用。適切なノードにメッセージがルーティング済みであることを表す
    * @param message
    */
  private[entityreplication] final case class Routed(message: Any)

  /** TypeName of Cluster Sharding on which Raft actors run */
  private[entityreplication] def raftShardingTypeName(
      replicationRegionTypeName: String,
      memberIndex: MemberIndex,
  ): String = {
    s"raft-shard-$replicationRegionTypeName-${memberIndex.role}"
  }

}

private[entityreplication] class ReplicationRegion(
    typeName: String,
    entityProps: EntityPropsProvider,
    settings: ClusterReplicationSettings,
    extractEntityId: ExtractEntityId,
    extractShardId: ExtractShardId,
    possibleShardIds: Set[ReplicationRegion.ShardId],
    commitLogStore: ActorRef,
) extends Actor
    with ActorLogging
    with Stash {
  import ReplicationRegion._

  private[this] val cluster = Cluster(context.system)

  // ReplicationRegion のログとして出力するため
  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = false) {
    case e =>
      val decide = super.supervisorStrategy.decider(e)
      decide match {
        case directive =>
          if (log.isErrorEnabled) log.error(e, "{}", directive)
      }
      decide
  }

  private[this] val allMemberIndexes: Set[MemberIndex] = settings.allMemberIndexes

  // protected[this]: for test purpose
  protected[this] val selfMemberIndex: MemberIndex = settings.selfMemberIndex

  // protected[this]: for test purpose
  protected[this] val otherMemberIndexes: Set[MemberIndex] = allMemberIndexes.filterNot(_ == selfMemberIndex)

  private[this] val regions: Map[MemberIndex, mutable.Set[Member]] =
    allMemberIndexes.map(i => i -> mutable.Set.empty[Member]).toMap

  private val disabledShards: Set[ShardId] = settings.raftSettings.disabledShards

  // TODO 変数名を実態にあったものに変更
  private[this] val shardingRouters: Map[MemberIndex, ActorRef] = allMemberIndexes.map { memberIndex =>
    def clusterReplicationShardId(message: Any): String = extractNormalizedShardIdInternal(message).raw
    val extractEntityId: ShardRegion.ExtractEntityId    = message => (clusterReplicationShardId(message), message)
    val extractShardId: ShardRegion.ExtractShardId = {
      case ShardRegion.StartEntity(id) => id
      case message                     => clusterReplicationShardId(message)
    }
    memberIndex -> {
      ClusterSharding(context.system).start(
        typeName = raftShardingTypeName(typeName, memberIndex),
        entityProps = createRaftActorProps(),
        settings = ClusterShardingSettings(settings.raftSettings.clusterShardingConfig)
          .withRole(memberIndex.role),
        extractEntityId,
        extractShardId,
      )
    }
  }.toMap

  /** *
    * 送信元と宛先のペアが同じ場合、メッセージ送信した順序で宛先にメッセージが到着するようにするため、ShardId ごとにメッセージの送信経路を固定する。
    * 利用可能な MemberIndex のセットが変動した場合、有効な MemberIndex 同士でメッセージの送信経路が入れ替わり、メッセージの到着順が入れ替わってしまわないように ConsistentHashing を使う。
    * 米 ConsistentHashing を使うと、無効になった MemberIndex に割り当てられていた経路のみが他の MemberIndex へ退避する形になる。
    */
  private[this] var stickyRoutingRouter: Router = {
    val hashMapping: ConsistentHashingRouter.ConsistentHashMapping = {
      case message => extractNormalizedShardIdInternal(message).underlying
    }
    Router(
      ConsistentHashingRoutingLogic(context.system, virtualNodesFactor = 256, hashMapping),
      shardingRouters.values.map(ActorRefRoutee).toVector,
    )
  }

  override def preStart(): Unit = {
    cluster.registerOnMemberUp {
      cluster.subscribe(
        self,
        initialStateMode = InitialStateAsSnapshot,
        classOf[MemberUp],
        classOf[MemberRemoved],
        classOf[UnreachableMember],
        classOf[ReachableMember],
      )
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    // shutdown only non-proxy ShardRegion
    shardingRouters(selfMemberIndex) ! GracefulShutdown
  }

  override def receive: Receive = initializing

  def initializing: Receive = {
    case snapshot: CurrentClusterState => handleClusterState(snapshot)
    case event: ClusterDomainEvent     => handleClusterDomainEvent(event)
    case _                             => stash()
  }

  def open: Receive = {
    case event: ClusterDomainEvent      => handleClusterDomainEvent(event)
    case routingCommand: RoutingCommand => handleRoutingCommand(routingCommand)
    case message                        => deliverMessage(message)
  }

  /**
    * 起動直後、メンバーの認識が細切れになるとメッセージの転送が不安定になるため
    */
  def handleClusterState(snapshot: CurrentClusterState): Unit = {
    snapshot.members.filter(_.status == MemberStatus.Up).foreach { member =>
      memberIndexOf(member).foreach(regions(_).add(member))
    }
    updateState()
    // This actor can determine whether it triggers all possible Raft actor starts
    // once a node in which the actor runs receives the current cluster state.
    triggerStartingAllPossibleRaftActorsIfNeeded()
  }

  def handleClusterDomainEvent(event: ClusterDomainEvent): Unit =
    event match {

      case MemberUp(member) =>
        memberIndexOf(member).foreach(regions(_).add(member))
        updateState()

      case ReachableMember(member) =>
        memberIndexOf(member).foreach(regions(_).add(member))
        updateState()

      case MemberRemoved(member, _) =>
        memberIndexOf(member).foreach(regions(_).remove(member))
        updateState()

      case UnreachableMember(member) =>
        memberIndexOf(member).foreach(regions(_).remove(member))
        updateState()

      case _ => unhandled(event)
    }

  def handleRoutingCommand(command: ReplicationRegion.RoutingCommand): Unit =
    command match {
      case Broadcast(message)              => allMemberIndexes.foreach(forwardMessageTo(_, Routed(message)))
      case BroadcastWithoutSelf(message)   => otherMemberIndexes.foreach(forwardMessageTo(_, Routed(message)))
      case DeliverTo(memberIndex, message) => forwardMessageTo(memberIndex, Routed(message))
      case DeliverSomewhere(message)       => stickyRoutingRouter.route(message, sender())
    }

  private[this] def forwardMessageTo(memberIndex: MemberIndex, message: Routed): Unit = {
    shardingRouters(memberIndex) forward message.message
  }

  def deliverMessage(message: Any): Unit = {
    if (extractShardId.isDefinedAt(message)) {
      val shardId = extractShardId(message)
      if (!disabledShards.contains(shardId)) {
        shardingRouters.values.foreach(
          // Don't forward StartEntity to prevent leaking StartEntityAck
          _.tell(ShardRegion.StartEntity(shardId), context.system.deadLetters),
        )
        handleRoutingCommand(DeliverSomewhere(Command(message)))
      } else if (log.isWarningEnabled) {
        log.warning(
          s"Following command had sent to disabled shards was dropped: {}(shardId={})",
          message.getClass.getName,
          shardId,
        )
      }
    } else {
      if (log.isWarningEnabled)
        log.warning("The message [{}] was dropped because its shard ID could not be extracted", message)
    }
  }

  private[this] def updateState(): Unit = {
    val availableRegions = regions.filter { case (_, members) => members.nonEmpty }
    stickyRoutingRouter =
      stickyRoutingRouter.withRoutees(availableRegions.keys.map(i => ActorRefRoutee(shardingRouters(i))).toVector)
    if (log.isInfoEnabled) log.info("Available cluster members changed: {}", availableRegions)
    // 一度 open になったら、その後は転送先のメンバーを増減させるだけ
    // 想定以上にメッセージが遅延して到着することを避けるため、メンバーが不足していたとしてもメッセージを stash しない
    if (availableRegions.size >= settings.raftSettings.quorumSize) {
      context.become(open)
      if (log.isDebugEnabled) log.debug("=== {} will be open ===", classOf[ReplicationRegion].getSimpleName)
      unstashAll()
    }
  }

  /** Triggers all possible [[RaftActor]] starts.
    *
    * This method will trigger all Raft actor starts on the following all conditions meet:
    *   - possibleShardIds is non-empty.
    *   - The node on which this actor runs is the oldest.
    */
  private def triggerStartingAllPossibleRaftActorsIfNeeded(): Unit = {
    // Only one raft actor runs on each shard.
    val possibleRaftActorIds = possibleShardIds
    // [Optimization]
    // Only the oldest node should trigger all possible Raft actor starts.
    // Newer nodes don't have to trigger starts since the oldest has already done such triggers.
    // This optimization reduces unnecessary message exchanges.
    val membersInSelfRegion = regions(selfMemberIndex).toVector
    def isOldest: Boolean = {
      val oldestMember = membersInSelfRegion.minOption(Member.ageOrdering)
      Option(cluster.selfMember) == oldestMember
    }
    val shouldTriggerStarting = possibleRaftActorIds.nonEmpty && isOldest
    if (shouldTriggerStarting) {
      // [Optimization]
      // The oldest node on each member index should trigger starts against only non-proxy Shard Region.
      // Each oldest node doesn't have to trigger starts against proxy Shard Region since another has already done such triggers.
      // This optimization reduces unnecessary message exchanges.
      val shardRegion = shardingRouters(selfMemberIndex)
      // This spawned actor will stop itself after all possible Raft actors start.
      context.spawn[Nothing](
        ReplicationRegionRaftActorStarter(shardRegion, possibleRaftActorIds, settings.raftSettings),
        "RaftActorStarter",
      )
      log.info(
        "Started ReplicationRegionRaftActorStarter for Shard Region [{}] (members: [{}])",
        shardRegion,
        membersInSelfRegion,
      )
    }
  }

  // protected[this]: for test purpose
  protected[this] def createRaftActorProps(): Props = {
    RaftActor.props(
      TypeName.from(typeName),
      extractNormalizedEntityId,
      entityProps,
      region = self,
      shardSnapshotStoreProps =
        ShardSnapshotStore.props(TypeName.from(typeName), settings.raftSettings, selfMemberIndex),
      selfMemberIndex,
      otherMemberIndexes,
      settings = settings.raftSettings,
      commitLogStore = commitLogStore,
    )
  }

  // protected[this]: for test purpose
  protected[this] def extractNormalizedEntityId: PartialFunction[Msg, (NormalizedEntityId, Msg)] =
    extractEntityId.andThen {
      case (entityId, msg) =>
        (NormalizedEntityId.from(entityId), msg)
    }

  private[this] def extractNormalizedShardId: ReplicationRegion.ExtractNormalizedShardId =
    extractShardId.andThen(i => NormalizedShardId.from(i))

  private[this] def extractNormalizedShardIdInternal: ReplicationRegion.ExtractNormalizedShardId = {
    case shardRequest: ShardRequest     => shardRequest.shardId
    case Command(cmd)                   => extractNormalizedShardId(cmd)
    case ForwardedCommand(Command(cmd)) => extractNormalizedShardId(cmd)
  }

  private[this] def memberIndexOf(member: Member): Option[MemberIndex] = {
    val maybeMemberIndex = allMemberIndexes.find(i => member.roles.contains(i.role))
    if (maybeMemberIndex.isEmpty) {
      if (log.isWarningEnabled)
        log.warning(
          "Member {} has no any role of MemberIndexes ({}). This member will be ignored",
          member,
          allMemberIndexes,
        )
    }
    maybeMemberIndex
  }
}
