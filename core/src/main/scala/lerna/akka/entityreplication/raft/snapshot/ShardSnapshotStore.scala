package lerna.akka.entityreplication.raft.snapshot

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex

private[entityreplication] object ShardSnapshotStore {

  def props(typeName: TypeName, settings: RaftSettings, selfMemberIndex: MemberIndex): Props =
    Props(new ShardSnapshotStore(typeName, settings, selfMemberIndex))

}

private[entityreplication] class ShardSnapshotStore(
    typeName: TypeName,
    settings: RaftSettings,
    selfMemberIndex: MemberIndex,
) extends Actor
    with ActorLogging {
  import SnapshotProtocol._

  override def receive: Receive = {
    case command: Command =>
      snapshotStore(command.entityId) forward command
  }

  def snapshotStore(entityId: NormalizedEntityId): ActorRef = {
    val name = snapshotStoreName(typeName, entityId)
    context
      .child(name).getOrElse(context.actorOf(SnapshotStore.props(typeName, entityId, settings, selfMemberIndex), name))
  }

  def snapshotStoreName(typeName: TypeName, entityId: NormalizedEntityId): String =
    s"SnapshotStore-${typeName.underlying}-${entityId.underlying}"
}
