package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpecLike }

final class CassandraPersistentActorRollbackSettingsSpec extends TestKitBase with WordSpecLike with Matchers {

  private val config: Config = ConfigFactory
    .parseString(s"""
      |custom_akka-persistence-cassandra = $${akka.persistence.cassandra} {
      |  read-profile = "custom_akka-persistence-cassandra-read-profile"
      |  write-profile = "custom_akka-persistence-cassandra-write-profile"
      |  journal {
      |    keyspace = "custom_akka"
      |    table = "custom_messages"
      |    target-partition-size = 1000000
      |  }
      |  events-by-tag {
      |    table = "custom_tag_views"
      |  }
      |  query {
      |    read-profile = "custom_akka-persistence-cassandra-query-profile"
      |    max-buffer-size = 1000
      |    deserialization-parallelism = 2
      |  }
      |  snapshot {
      |    write-profile = "custom_akka-persistence-cassandra-snapshot-write-profile"
      |    keyspace = "custom_akka_snapshot"
      |    table = "custom_snapshots"
      |  }
      |}
      |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  private val customPluginLocation: String =
    "custom_akka-persistence-cassandra"

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName, config)

  "CassandraPersistentActorRollbackSettings" should {

    "load the given custom plugin config" in {
      val settings =
        CassandraPersistentActorRollbackSettings(system, pluginLocation = customPluginLocation, dryRun = true)

      // Journal settings
      settings.journal.readProfile should be("custom_akka-persistence-cassandra-read-profile")
      settings.journal.writeProfile should be("custom_akka-persistence-cassandra-write-profile")
      settings.journal.keyspace should be("custom_akka")
      settings.journal.table should be("custom_messages")
      settings.journal.targetPartitionSize should be(1_000_000)
      settings.journal.tableName should be("custom_akka.custom_messages")

      // Query settings
      settings.query.readProfile should be("custom_akka-persistence-cassandra-query-profile")
      settings.query.maxBufferSize should be(1000)
      settings.query.deserializationParallelism should be(2)

      // EventsByTag Settings
      settings.eventsByTag.keyspace should be("custom_akka")
      settings.eventsByTag.table should be("custom_tag_views")

      // Snapshot settings
      settings.snapshot.writeProfile should be("custom_akka-persistence-cassandra-snapshot-write-profile")
      settings.snapshot.keyspace should be("custom_akka_snapshot")
      settings.snapshot.table should be("custom_snapshots")
      settings.snapshot.tableName should be("custom_akka_snapshot.custom_snapshots")
    }

  }

  "CassandraPersistentActorRollbackSettings.cleanup" should {

    "return a CleanupSettings whose pluginLocation is equal to CassandraPersistentActorRollbackSettings.pluginLocation" in {
      val settings = CassandraPersistentActorRollbackSettings(system, customPluginLocation, dryRun = true)
      settings.cleanup.pluginLocation should be(settings.pluginLocation)
    }

    "return a CleanupSettings whose dryRun is equal to CassandraPersistentActorRollbackSettings.dryRun" in {
      val settings = CassandraPersistentActorRollbackSettings(system, customPluginLocation, dryRun = false)
      settings.cleanup.dryRun should be(settings.dryRun)
    }

  }

  "CassandraPersistentActorRollbackSettings.reconciliation" should {

    "return a ReconciliationSettings whose pluginLocation is equal to CassandraPersistentActorRollbackSettings.pluginLocation" in {
      val settings = CassandraPersistentActorRollbackSettings(system, customPluginLocation, dryRun = true)
      settings.reconciliation.pluginLocation should be(settings.pluginLocation)
    }

    "return a ReconciliationSettings whose readProfile is equal to CassandraPersistentActorRollbackSettings.journal.readProfile" in {
      val settings = CassandraPersistentActorRollbackSettings(system, customPluginLocation, dryRun = true)
      settings.reconciliation.readProfile should be(settings.journal.readProfile)
    }

    "return a ReconciliationSettings whose writeProfile is equal to CassandraPersistentActorRollbackSettings.journal.writeProfile" in {
      val settings = CassandraPersistentActorRollbackSettings(system, customPluginLocation, dryRun = true)
      settings.reconciliation.writeProfile should be(settings.journal.writeProfile)
    }

  }

  "CassandraPersistentActorRollbackSettings.queries" should {

    "return a CassandraPersistenceQueriesSettings whose pluginLocation is equal to CassandraPersistentActorRollbackSettings.pluginLocation" in {
      val settings = CassandraPersistentActorRollbackSettings(system, customPluginLocation, dryRun = true)
      settings.queries.pluginLocation should be(settings.pluginLocation)
    }

  }

}
