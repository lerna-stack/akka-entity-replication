package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpec }

final class CassandraJournalSettingsSpec extends WordSpec with Matchers {

  private val defaultPluginConfig: Config =
    ConfigFactory.load().getConfig("akka.persistence.cassandra")

  private val customPluginConfig: Config = ConfigFactory.parseString("""
      |read-profile = "custom_akka-persistence-cassandra-read-profile"
      |write-profile = "custom_akka-persistence-cassandra-write-profile"
      |journal {
      |  keyspace = "custom_akka"
      |  table = "custom_messages"
      |  metadata-table = "custom_metadata"
      |  target-partition-size = 1000000
      |}
      |""".stripMargin)

  "CassandraJournalSettings" should {

    "load the default config" in {
      val settings = CassandraJournalSettings(defaultPluginConfig)
      settings.readProfile should be("akka-persistence-cassandra-profile")
      settings.writeProfile should be("akka-persistence-cassandra-profile")
      settings.keyspace should be("akka")
      settings.table should be("messages")
      settings.metadataTable should be("metadata")
      settings.targetPartitionSize should be(500_000)
    }

    "load the given custom config" in {
      val settings = CassandraJournalSettings(customPluginConfig)
      settings.readProfile should be("custom_akka-persistence-cassandra-read-profile")
      settings.writeProfile should be("custom_akka-persistence-cassandra-write-profile")
      settings.keyspace should be("custom_akka")
      settings.table should be("custom_messages")
      settings.metadataTable should be("custom_metadata")
      settings.targetPartitionSize should be(1_000_000)
    }

    "throw an IllegalArgumentException if the given journal.target-partition-size is 0" in {
      val invalidPluginConfig =
        ConfigFactory
          .parseString("""
            |journal.target-partition-size = 0
            |""".stripMargin)
          .withFallback(defaultPluginConfig)
      val exception = intercept[IllegalArgumentException] {
        CassandraJournalSettings(invalidPluginConfig)
      }
      exception.getMessage should be(
        "requirement failed: journal.target-partition-size [0] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given journal.target-partition-size is negative" in {
      val invalidPluginConfig =
        ConfigFactory
          .parseString("""
            |journal.target-partition-size = -1
            |""".stripMargin)
          .withFallback(defaultPluginConfig)
      val exception = intercept[IllegalArgumentException] {
        CassandraJournalSettings(invalidPluginConfig)
      }
      exception.getMessage should be(
        "requirement failed: journal.target-partition-size [-1] should be greater than 0",
      )
    }

  }

  "CassandraJournalSettings.tableName" should {

    "return the table name qualified with the keyspace name" in {
      val settings = CassandraJournalSettings(customPluginConfig)
      settings.keyspace should be("custom_akka")
      settings.table should be("custom_messages")
      settings.tableName should be("custom_akka.custom_messages")
    }

  }

  "CassandraJournalSettings.metadataTableName" should {

    "return the metadata table name qualified with the keyspace name" in {
      val settings = CassandraJournalSettings(customPluginConfig)
      settings.keyspace should be("custom_akka")
      settings.metadataTable should be("custom_metadata")
      settings.metadataTableName should be("custom_akka.custom_metadata")
    }

  }

}
