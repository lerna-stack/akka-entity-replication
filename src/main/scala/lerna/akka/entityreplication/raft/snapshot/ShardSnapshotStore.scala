package lerna.akka.entityreplication.raft.snapshot

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import lerna.akka.entityreplication.model.NormalizedEntityId
import lerna.akka.entityreplication.raft.RaftSettings
import lerna.akka.entityreplication.raft.routing.MemberIndex

object ShardSnapshotStore {

  def props(typeName: String, settings: RaftSettings, selfMemberIndex: MemberIndex): Props =
    Props(new ShardSnapshotStore(typeName, settings, selfMemberIndex))

}

class ShardSnapshotStore(typeName: String, settings: RaftSettings, selfMemberIndex: MemberIndex)
    extends Actor
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

  def snapshotStoreName(typeName: String, entityId: NormalizedEntityId): String =
    s"SnapshotStore-${typeName}-${entityId.underlying}"
}
