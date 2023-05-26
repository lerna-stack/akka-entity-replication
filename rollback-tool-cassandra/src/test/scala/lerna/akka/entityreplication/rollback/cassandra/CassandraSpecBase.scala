package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.rollback.cassandra.testkit.PersistenceCassandraConfigProvider
import lerna.akka.entityreplication.rollback.testkit.{
  PatienceConfigurationForTestKitBase,
  PersistenceInitializationAwaiter,
}
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.util.concurrent.atomic.AtomicLong

abstract class CassandraSpecBase(
    name: String,
    overrideConfig: Config = ConfigFactory.empty,
) extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll
    with PatienceConfigurationForTestKitBase
    with PersistenceCassandraConfigProvider {

  /** Keyspace the journal plugin use */
  protected def journalKeyspace: String = name

  /** Keyspace the snapshot plugin use */
  protected def snapshotKeyspace: String = name

  override implicit lazy val system: ActorSystem = {
    val config =
      overrideConfig
        .withFallback(persistenceCassandraConfig(journalKeyspace, snapshotKeyspace, autoCreate = true))
        .withFallback(ConfigFactory.load())
        .resolve()
    ActorSystem(name, config)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // NOTE: Cassandra is running until all tests are done.
    CassandraLauncher.main(Array(s"${cassandraPort}", "true"))
    PersistenceInitializationAwaiter(system).awaitInit()
  }

  override def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }

  private val currentPersistenceId: AtomicLong = new AtomicLong(0)

  /** Returns the next (unique) persistence ID */
  protected def nextPersistenceId(): String = {
    s"$name-${currentPersistenceId.incrementAndGet()}"
  }

  private val currentTagId: AtomicLong = new AtomicLong(0)

  /** Returns the next unique tag name */
  protected def nextUniqueTag(): String = {
    s"$name-tag-${currentTagId.incrementAndGet()}"
  }

}
