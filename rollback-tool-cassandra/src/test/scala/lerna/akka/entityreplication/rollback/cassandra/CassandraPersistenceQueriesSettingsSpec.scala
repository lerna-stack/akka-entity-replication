package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpecLike }

final class CassandraPersistenceQueriesSettingsSpec extends TestKitBase with WordSpecLike with Matchers {

  private val config: Config = ConfigFactory
    .parseString(s"""
      |custom.akka.persistence.cassandra = $${akka.persistence.cassandra} {
      |  read-profile = "custom_akka-persistence-cassandra-read-profile"
      |  write-profile = "custom_akka-persistence-cassandra-write-profile"
      |  query {
      |    read-profile = "custom_akka-persistence-cassandra-query-profile"
      |  }
      |}
      |""".stripMargin)
    .withFallback(ConfigFactory.load())
    .resolve()

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName, config)

  "CassandraPersistenceQueriesSettings.resolveJournalSettings" should {

    "resolve the plugin location and then return the journal plugin settings" in {
      val settings        = new CassandraPersistenceQueriesSettings("custom.akka.persistence.cassandra")
      val journalSettings = settings.resolveJournalSettings(system)
      journalSettings.readProfile should be("custom_akka-persistence-cassandra-read-profile")
      journalSettings.writeProfile should be("custom_akka-persistence-cassandra-write-profile")
    }

  }

  "CassandraPersistenceQueriesSettings.resolveQuerySettings" should {

    "resolve the plugin location and then return the query plugin settings" in {
      val settings      = new CassandraPersistenceQueriesSettings("custom.akka.persistence.cassandra")
      val querySettings = settings.resolveQuerySettings(system)
      querySettings.readProfile should be("custom_akka-persistence-cassandra-query-profile")
    }

  }

}
