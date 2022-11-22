package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration.DurationInt

final class CassandraRaftShardRollbackSettingsSpec extends TestKitBase with WordSpecLike with Matchers {

  private val config: Config = ConfigFactory
    .parseString(s"""
      |custom_raft_akka-persistence-cassandra = $${akka.persistence.cassandra}
      |custom_raft-eventsourced_akka-persistence-cassandra = $${akka.persistence.cassandra}
      |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName, config)

  private val customConfig: Config = ConfigFactory.parseString("""
      |dry-run = false
      |log-progress-every = 200
      |clock-out-of-sync-tolerance = 20s
      |read-parallelism = 2
      |write-parallelism = 2
      |cassandra.raft-persistence-plugin-location = "custom_raft_akka-persistence-cassandra"
      |cassandra.raft-eventsourced-persistence-plugin-location = "custom_raft-eventsourced_akka-persistence-cassandra"
      |""".stripMargin)

  "CassandraRaftShardRollbackSettings" should {

    "load the default config from the given system" in {
      val settings = CassandraRaftShardRollbackSettings(system)
      settings.rollbackSettings.dryRun should be(true)
      settings.rollbackSettings.clockOutOfSyncTolerance should be(10.seconds)
      settings.rollbackSettings.readParallelism should be(1)
      settings.rollbackSettings.writeParallelism should be(1)
      settings.raftPersistenceRollbackSettings.pluginLocation should be(
        "akka.persistence.cassandra",
      )
      settings.raftPersistenceRollbackSettings.dryRun should be(true)
      settings.raftEventSourcedPersistenceRollbackSettings.pluginLocation should be(
        "akka.persistence.cassandra",
      )
      settings.raftEventSourcedPersistenceRollbackSettings.dryRun should be(true)
    }

    "load the given custom config" in {
      val settings = CassandraRaftShardRollbackSettings(system, customConfig)
      settings.rollbackSettings.dryRun should be(false)
      settings.rollbackSettings.logProgressEvery should be(200)
      settings.rollbackSettings.clockOutOfSyncTolerance should be(20.seconds)
      settings.rollbackSettings.readParallelism should be(2)
      settings.rollbackSettings.writeParallelism should be(2)
      settings.raftPersistenceRollbackSettings.pluginLocation should be(
        "custom_raft_akka-persistence-cassandra",
      )
      settings.raftPersistenceRollbackSettings.dryRun should be(false)
      settings.raftEventSourcedPersistenceRollbackSettings.pluginLocation should be(
        "custom_raft-eventsourced_akka-persistence-cassandra",
      )
      settings.raftEventSourcedPersistenceRollbackSettings.dryRun should be(false)
    }

  }

  "CassandraRaftShardRollbackSettings.raftPersistenceRollbackSettings.pluginLocation" should {

    "be equal to CassandraRaftShardRollbackSettings.raftPersistencePluginLocation" in {
      val settings = CassandraRaftShardRollbackSettings(system, customConfig)
      settings.raftPersistenceRollbackSettings.pluginLocation should be(
        settings.raftPersistencePluginLocation,
      )
    }

  }

  "CassandraRaftShardRollbackSettings.raftEventSourcedPersistenceRollbackSettings.pluginLocation" should {

    "be equal to CassandraRaftShardRollbackSettings.raftEventSourcedPersistencePluginLocation" in {
      val settings = CassandraRaftShardRollbackSettings(system, customConfig)
      settings.raftEventSourcedPersistenceRollbackSettings.pluginLocation should be(
        settings.raftEventSourcedPersistencePluginLocation,
      )
    }

  }

}
