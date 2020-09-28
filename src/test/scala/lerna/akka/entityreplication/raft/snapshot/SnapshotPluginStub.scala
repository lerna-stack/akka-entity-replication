package lerna.akka.entityreplication.raft.snapshot

import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Future

object SnapshotPluginStub {

  private[this] val commonSnapshotStubConfig = ConfigFactory.parseString("""
                   | lerna.akka.entityreplication.raft.persistence.snapshot-store.plugin = snapshot-store-stub
                   | snapshot-store-stub {
                   |   # Class name of the plugin.
                   |   class = null # have to set
                   |   # Dispatcher for the plugin actor.
                   |   plugin-dispatcher = "akka.persistence.dispatchers.default-plugin-dispatcher"
                   | }
                   |""".stripMargin)

  val brokenLoadingSnapshotConfig: Config = ConfigFactory
    .parseString(s"""
                   | snapshot-store-stub.class = ${classOf[BrokenLoadingSnapshotPluginStub].getCanonicalName}
                   |""".stripMargin).withFallback(commonSnapshotStubConfig)

  val brokenSavingSnapshotConfig: Config = ConfigFactory
    .parseString(s"""
                  | snapshot-store-stub.class = ${classOf[BrokenSavingSnapshotPluginStub].getCanonicalName}
                  |""".stripMargin).withFallback(commonSnapshotStubConfig)
}

class BrokenLoadingSnapshotPluginStub
    extends SnapshotPluginStub(loadAsyncResponse = Future.failed(new RuntimeException("loading snapshot failure")))
class BrokenSavingSnapshotPluginStub
    extends SnapshotPluginStub(saveAsyncResponse = Future.failed(new RuntimeException("saving snapshot failure")))

/**
  * [[akka.persistence.snapshot.SnapshotStore]] のスた部
  * @see https://doc.akka.io/docs/akka/current/persistence-journals.html#snapshot-store-plugin-api
  */
class SnapshotPluginStub(
    loadAsyncResponse: Future[Option[SelectedSnapshot]] = Future.successful(None),
    saveAsyncResponse: Future[Unit] = Future.successful(()),
) extends akka.persistence.snapshot.SnapshotStore {

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    loadAsyncResponse

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = saveAsyncResponse

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    Future.failed(new UnsupportedOperationException("deleteAsync unsupported"))

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    Future.failed(new UnsupportedOperationException("deleteAsync unsupported"))
}
