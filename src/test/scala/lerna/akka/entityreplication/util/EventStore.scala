package lerna.akka.entityreplication.util

import akka.Done
import akka.actor.{ Actor, Props }
import akka.persistence.{ PersistentActor, RuntimePluginConfig }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.ClusterReplicationSettings

object EventStore {
  def props(settings: ClusterReplicationSettings): Props = Props(new EventStore(settings))
  final case class PersistEvents(events: Seq[Any])
}

class EventStore(settings: ClusterReplicationSettings) extends PersistentActor with RuntimePluginConfig {
  import EventStore._

  override def journalPluginId: String = settings.raftSettings.journalPluginId

  override def journalPluginConfig: Config = settings.raftSettings.journalPluginAdditionalConfig

  override def snapshotPluginId: String = settings.raftSettings.snapshotStorePluginId

  override def snapshotPluginConfig: Config = ConfigFactory.empty()

  override def persistenceId: String = getClass.getCanonicalName

  override def receiveRecover: Receive = Actor.emptyBehavior

  private[this] var persisting: Int = 0

  override def receiveCommand: Receive = {
    case cmd: PersistEvents if cmd.events.isEmpty =>
      sender() ! Done
    case cmd: PersistEvents =>
      persisting = cmd.events.size
      persistAll(cmd.events.toVector) { _ =>
        persisting -= 1
        if (persisting == 0) {
          sender() ! Done
        }
      }
  }
}
