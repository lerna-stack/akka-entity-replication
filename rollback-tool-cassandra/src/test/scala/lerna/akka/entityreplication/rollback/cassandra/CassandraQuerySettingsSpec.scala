package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpec }

final class CassandraQuerySettingsSpec extends WordSpec with Matchers {

  private val defaultPluginConfig: Config =
    ConfigFactory.load().getConfig("akka.persistence.cassandra")

  private val customPluginConfig: Config = ConfigFactory.parseString("""
      |query {
      |  read-profile = "custom_akka-persistence-cassandra-read-profile"
      |  max-buffer-size = 1000
      |  deserialization-parallelism = 2
      |}
      |""".stripMargin)

  "CassandraQuerySettings" should {

    "load the default config" in {
      val settings = CassandraQuerySettings(defaultPluginConfig)
      settings.readProfile should be("akka-persistence-cassandra-profile")
      settings.maxBufferSize should be(500)
      settings.deserializationParallelism should be(1)
    }

    "load the given config" in {
      val settings = CassandraQuerySettings(customPluginConfig)
      settings.readProfile should be("custom_akka-persistence-cassandra-read-profile")
      settings.maxBufferSize should be(1000)
      settings.deserializationParallelism should be(2)
    }

    "throw an IllegalArgumentException if the given query.max-buffer-size is 0" in {
      val invalidPluginConfig =
        ConfigFactory
          .parseString("""
            |query.max-buffer-size = 0
            |""".stripMargin)
          .withFallback(defaultPluginConfig)
      val exception = intercept[IllegalArgumentException] {
        CassandraQuerySettings(invalidPluginConfig)
      }
      exception.getMessage should be("requirement failed: query.max-buffer-size [0] should be greater than 0")
    }

    "throw an IllegalArgumentException if the given query.max-buffer-size is negative" in {
      val invalidPluginConfig =
        ConfigFactory
          .parseString("""
            |query.max-buffer-size = -1
            |""".stripMargin)
          .withFallback(defaultPluginConfig)
      val exception = intercept[IllegalArgumentException] {
        CassandraQuerySettings(invalidPluginConfig)
      }
      exception.getMessage should be("requirement failed: query.max-buffer-size [-1] should be greater than 0")
    }

    "throw an IllegalArgumentException if the given query.deserialization-parallelism is 0" in {
      val invalidPluginConfig =
        ConfigFactory
          .parseString("""
            |query.deserialization-parallelism = 0
            |""".stripMargin)
          .withFallback(defaultPluginConfig)
      val exception = intercept[IllegalArgumentException] {
        CassandraQuerySettings(invalidPluginConfig)
      }
      exception.getMessage should be(
        "requirement failed: query.deserialization-parallelism [0] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given query.deserialization-parallelism is negative" in {
      val invalidPluginConfig =
        ConfigFactory
          .parseString("""
            |query.deserialization-parallelism = -1
            |""".stripMargin)
          .withFallback(defaultPluginConfig)
      val exception = intercept[IllegalArgumentException] {
        CassandraQuerySettings(invalidPluginConfig)
      }
      exception.getMessage should be(
        "requirement failed: query.deserialization-parallelism [-1] should be greater than 0",
      )
    }

  }

}
