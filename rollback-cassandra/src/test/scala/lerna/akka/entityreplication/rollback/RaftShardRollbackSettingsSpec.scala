package lerna.akka.entityreplication.rollback

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration.DurationInt

final class RaftShardRollbackSettingsSpec extends TestKitBase with WordSpecLike with Matchers {

  override implicit val system: ActorSystem =
    ActorSystem(getClass.getSimpleName)

  private val customRollbackConfig: Config = ConfigFactory.parseString("""
      |dry-run = false
      |clock-out-of-sync-tolerance = 5s
      |read-parallelism = 3
      |write-parallelism = 2
      |log-progress-every = 200
      |""".stripMargin)

  "RaftShardRollbackSettings" should {

    "load the default config from the given system" in {
      val settings = RaftShardRollbackSettings(system)
      settings.dryRun should be(true)
      settings.logProgressEvery should be(100)
      settings.clockOutOfSyncTolerance should be(10.seconds)
      settings.readParallelism should be(1)
      settings.writeParallelism should be(1)
    }

    "load the given custom config" in {
      val settings = RaftShardRollbackSettings(customRollbackConfig)
      settings.dryRun should be(false)
      settings.logProgressEvery should be(200)
      settings.clockOutOfSyncTolerance should be(5.seconds)
      settings.readParallelism should be(3)
      settings.writeParallelism should be(2)
    }

    "throw an IllegalArgumentException if the given log-progress-every is 0" in {
      val invalidRollbackConfig =
        ConfigFactory
          .parseString("""
            |log-progress-every = 0
            |""".stripMargin)
          .withFallback(customRollbackConfig)
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackSettings(invalidRollbackConfig)
      }
      exception.getMessage should be(
        "requirement failed: log-progress-every [0] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given log-progress-every is negative" in {
      val invalidRollbackConfig =
        ConfigFactory
          .parseString("""
            |log-progress-every = -1
            |""".stripMargin)
          .withFallback(customRollbackConfig)
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackSettings(invalidRollbackConfig)
      }
      exception.getMessage should be(
        "requirement failed: log-progress-every [-1] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given read-parallelism is 0" in {
      val invalidRollbackConfig =
        ConfigFactory
          .parseString("""
            |read-parallelism = 0
            |""".stripMargin)
          .withFallback(customRollbackConfig)
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackSettings(invalidRollbackConfig)
      }
      exception.getMessage should be(
        "requirement failed: read-parallelism [0] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given read-parallelism is negative" in {
      val invalidRollbackConfig =
        ConfigFactory
          .parseString("""
            |read-parallelism = -1
            |""".stripMargin)
          .withFallback(customRollbackConfig)
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackSettings(invalidRollbackConfig)
      }
      exception.getMessage should be(
        "requirement failed: read-parallelism [-1] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given writeParallelism is 0" in {
      val invalidRollbackConfig =
        ConfigFactory
          .parseString("""
            |write-parallelism = 0
            |""".stripMargin)
          .withFallback(customRollbackConfig)
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackSettings(invalidRollbackConfig)
      }
      exception.getMessage should be(
        "requirement failed: write-parallelism [0] should be greater than 0",
      )
    }

    "throw an IllegalArgumentException if the given writeParallelism is negative" in {
      val invalidRollbackConfig =
        ConfigFactory
          .parseString("""
            |write-parallelism = -1
            |""".stripMargin)
          .withFallback(customRollbackConfig)
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackSettings(invalidRollbackConfig)
      }
      exception.getMessage should be(
        "requirement failed: write-parallelism [-1] should be greater than 0",
      )
    }

  }

}
