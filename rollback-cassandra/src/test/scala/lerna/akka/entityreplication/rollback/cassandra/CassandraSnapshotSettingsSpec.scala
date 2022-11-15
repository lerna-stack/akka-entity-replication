package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpec }

final class CassandraSnapshotSettingsSpec extends WordSpec with Matchers {

  private val defaultPluginConfig: Config =
    ConfigFactory.load().getConfig("akka.persistence.cassandra")

  private val customPluginConfig: Config = ConfigFactory.parseString("""
      |snapshot {
      |  write-profile = "custom_akka-persistence-cassandra-snapshot-write-profile"
      |  keyspace = "custom_akka_snapshot"
      |  table = "custom_snapshots"
      |}
      |""".stripMargin)

  "CassandraSnapshotSettings" should {

    "load the default config" in {
      val settings = CassandraSnapshotSettings(defaultPluginConfig)
      settings.writeProfile should be("akka-persistence-cassandra-snapshot-profile")
      settings.keyspace should be("akka_snapshot")
      settings.table should be("snapshots")
    }

    "load the given custom config" in {
      val settings = CassandraSnapshotSettings(customPluginConfig)
      settings.writeProfile should be("custom_akka-persistence-cassandra-snapshot-write-profile")
      settings.keyspace should be("custom_akka_snapshot")
      settings.table should be("custom_snapshots")
    }

  }

  "CassandraSnapshotSettings.tableName" should {

    "return the table name qualified with the keyspace name" in {
      val settings = CassandraSnapshotSettings(customPluginConfig)
      settings.keyspace should be("custom_akka_snapshot")
      settings.table should be("custom_snapshots")
      settings.tableName should be("custom_akka_snapshot.custom_snapshots")
    }

  }

}
