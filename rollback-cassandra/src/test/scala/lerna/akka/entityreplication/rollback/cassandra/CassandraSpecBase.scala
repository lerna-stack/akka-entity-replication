package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.rollback.cassandra.testkit.FirstTimeBucket
import lerna.akka.entityreplication.rollback.testkit.PatienceConfigurationForTestKitBase
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicLong

object CassandraSpecBase {

  private def firstTimeBucket: String = {
    val time = ZonedDateTime.now().minusMinutes(1)
    FirstTimeBucket.format(time)
  }

  private val CassandraPort = 9042

  private def config(journalKeyspace: String, snapshotKeyspace: String): Config = ConfigFactory.parseString(s"""
      |akka.persistence.journal.plugin = akka.persistence.cassandra.journal
      |akka.persistence.snapshot-store.plugin = akka.persistence.cassandra.snapshot
      |
      |akka.persistence.cassandra.journal.keyspace = "$journalKeyspace"
      |akka.persistence.cassandra.journal.keyspace-autocreate = true
      |akka.persistence.cassandra.journal.tables-autocreate = true
      |
      |akka.persistence.cassandra.snapshot.keyspace = "$snapshotKeyspace"
      |akka.persistence.cassandra.snapshot.keyspace-autocreate = true
      |akka.persistence.cassandra.snapshot.tables-autocreate = true
      |
      |akka.persistence.cassandra.events-by-tag.eventual-consistency-delay = 300ms
      |akka.persistence.cassandra.events-by-tag.first-time-bucket = "$firstTimeBucket"
      |
      |datastax-java-driver {
      |  advanced.reconnect-on-init = true
      |  basic.contact-points = ["127.0.0.1:$CassandraPort"]
      |  basic.load-balancing-policy.local-datacenter = "datacenter1"
      |}
      |""".stripMargin)

}

abstract class CassandraSpecBase(
    name: String,
    overrideConfig: Config = ConfigFactory.empty,
) extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll
    with PatienceConfigurationForTestKitBase {

  /** Keyspace the journal plugin use */
  protected def journalKeyspace: String = name

  /** Keyspace the snapshot plugin use */
  protected def snapshotKeyspace: String = name

  override implicit lazy val system: ActorSystem = {
    val config =
      overrideConfig
        .withFallback(CassandraSpecBase.config(journalKeyspace, snapshotKeyspace))
        .withFallback(ConfigFactory.load())
        .resolve()
    ActorSystem(name, config)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // NOTE: Cassandra is running until all tests are done.
    CassandraLauncher.main(Array(s"${CassandraSpecBase.CassandraPort}", "true"))
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
