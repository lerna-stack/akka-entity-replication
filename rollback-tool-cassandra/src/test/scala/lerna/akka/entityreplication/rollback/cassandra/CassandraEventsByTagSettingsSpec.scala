package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpec }

final class CassandraEventsByTagSettingsSpec extends WordSpec with Matchers {

  private val defaultPluginConfig: Config =
    ConfigFactory.load().getConfig("akka.persistence.cassandra")

  private val customPluginConfig: Config = ConfigFactory.parseString("""
      |journal {
      |  keyspace = "custom_akka"
      |}
      |events-by-tag {
      |  table = "custom_tag_views"
      |}
      |""".stripMargin)

  "CassandraEventsByTagSettings" should {

    "load the default config" in {
      val settings = CassandraEventsByTagSettings(defaultPluginConfig)
      settings.keyspace should be("akka")
      settings.table should be("tag_views")
    }

    "load the given custom config" in {
      val settings = CassandraEventsByTagSettings(customPluginConfig)
      settings.keyspace should be("custom_akka")
      settings.table should be("custom_tag_views")
    }

  }

  "CassandraEventsByTagSettings.tagViewsTableName" should {

    "return the tag_views table name qualified with the keyspace name" in {
      val settings = CassandraEventsByTagSettings(customPluginConfig)
      settings.keyspace should be("custom_akka")
      settings.table should be("custom_tag_views")
      settings.tagViewsTableName should be("custom_akka.custom_tag_views")
    }

  }

  "CassandraEventsByTagSettings.tagWriteProgressTableName" should {

    "return the tag_write_progress table name qualified with the keyspace name" in {
      val settings = CassandraEventsByTagSettings(customPluginConfig)
      settings.keyspace should be("custom_akka")
      settings.tagWriteProgressTableName should be("custom_akka.tag_write_progress")
    }

  }

  "CassandraEventsByTagSettings.tagScanningTableName" should {

    "return the tag_scanning table name qualified with the keyspace name" in {
      val settings = CassandraEventsByTagSettings(customPluginConfig)
      settings.keyspace should be("custom_akka")
      settings.tagScanningTableName should be("custom_akka.tag_scanning")
    }

  }

}
